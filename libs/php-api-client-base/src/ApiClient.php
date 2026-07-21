<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use JsonException;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Keboola\ApiClientBase\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;
use Throwable;

class ApiClient
{
    private readonly GuzzleClient $httpClient;
    private readonly ErrorMessageResolverInterface $errorMessageResolver;

    /** @var class-string<ClientException> */
    private readonly string $exceptionClass;

    /**
     * @param non-empty-string|null $baseUrl
     * @param list<int> $retryableStatusCodes Non-5xx status codes to also retry (e.g. [429]).
     * @param class-string<ClientException> $exceptionClass Exception class thrown on failure; a
     *     ClientException subclass lets callers identify the failing service. It is instantiated
     *     inline at each throw site so the exception originates there (clean stack trace). A
     *     subclass that wants richer construction does it in its own constructor, which receives
     *     ($message, $code, $previous, $statusCode, $responseBody).
     */
    public function __construct(
        ?string $baseUrl,
        RequestAuthenticatorInterface $authenticator,
        ?ApiClientOptions $options = null,
        ?ErrorMessageResolverInterface $errorMessageResolver = null,
        array $retryableStatusCodes = [],
        string $exceptionClass = ClientException::class,
    ) {
        $options ??= new ApiClientOptions();
        $this->errorMessageResolver = $errorMessageResolver ?? new DefaultErrorMessageResolver();
        $this->exceptionClass = $exceptionClass;
        $logger = $options->logger ?? new NullLogger();

        $stack = $options->requestHandler instanceof HandlerStack
            ? $options->requestHandler
            : HandlerStack::create($options->requestHandler);

        // Push order matters: Guzzle resolves the stack so the FIRST-pushed
        // middleware is OUTERMOST. Push retry before auth so auth sits INSIDE
        // the retry loop and re-executes on every attempt — this lets
        // file-/token-backed authenticators (e.g. the projected SA token) be
        // re-resolved per retry.
        if ($options->backoffMaxTries > 0) {
            $stack->push(Middleware::retry(new RetryDecider(
                $options->backoffMaxTries,
                $logger,
                $retryableStatusCodes,
            )));
        }

        // Apply auth as a middleware that turns a thrown authenticator error into a rejected
        // promise (rather than letting it escape the stack synchronously). This way an
        // authenticator failure — e.g. a projected SA token momentarily unreadable during
        // rotation — flows through the retry middleware above and the normal error handling
        // below (surfacing as ClientException), instead of bypassing both.
        $stack->push(
            /**
             * @param callable(RequestInterface, array<string, mixed>): PromiseInterface $handler
             * @return callable(RequestInterface, array<string, mixed>): PromiseInterface
             */
            static function (callable $handler) use ($authenticator): callable {
                return static function (
                    RequestInterface $request,
                    array $options,
                ) use (
                    $handler,
                    $authenticator,
                ): PromiseInterface {
                    try {
                        $request = $authenticator($request);
                    } catch (Throwable $e) {
                        return Create::rejectionFor($e);
                    }

                    return $handler($request, $options);
                };
            },
            'auth',
        );

        $stack->push(Middleware::log(
            $logger,
            new MessageFormatter('{method} {uri} : {code} {res_header_Content-Length}'),
            LogLevel::DEBUG,
        ));

        $this->httpClient = new GuzzleClient([
            'base_uri' => $baseUrl === null ? null : rtrim($baseUrl, '/') . '/',
            'handler' => $stack,
            'headers' => [
                'User-Agent' => $options->userAgent,
            ],
            'connect_timeout' => $options->connectTimeout,
            'timeout' => $options->requestTimeout,
        ]);
    }

    /**
     * @param array<string, mixed> $options
     */
    public function sendRequest(RequestInterface $request, array $options = []): ResponseInterface
    {
        try {
            return $this->httpClient->send($request, $options);
        } catch (RequestException $e) {
            throw $this->processRequestException($e);
        } catch (GuzzleException $e) {
            throw new $this->exceptionClass($e->getMessage(), 0, $e, null, null);
        } catch (Throwable $e) {
            // Non-Guzzle failure bubbling out of the handler stack — e.g. an authenticator
            // that could not produce credentials (after retries are exhausted).
            throw new $this->exceptionClass(trim($e->getMessage()), 0, $e, null, null);
        }
    }

    /**
     * @template T of ResponseModelInterface
     * @param class-string<T> $responseClass
     * @param array<string, mixed> $options
     * @return ($isList is true ? list<T> : T)
     */
    public function sendRequestAndMapResponse(
        RequestInterface $request,
        string $responseClass,
        array $options = [],
        bool $isList = false,
    ) {
        $response = $this->sendRequest($request, $options);
        $body = $response->getBody()->getContents();

        try {
            $data = Json::decodeArray($body);
        } catch (JsonException $e) {
            throw new $this->exceptionClass(
                'Response is not valid JSON: ' . $e->getMessage(),
                0,
                $e,
                $response->getStatusCode(),
                $body,
            );
        }

        try {
            if ($isList) {
                /** @var list<array<string, mixed>> $data */
                return array_values(array_map(
                    static fn(array $item): mixed => $responseClass::fromResponseData($item),
                    $data,
                ));
            }
            return $responseClass::fromResponseData($data);
        } catch (Throwable $e) {
            throw new $this->exceptionClass(
                'Failed to map response data: ' . $e->getMessage(),
                0,
                $e,
                $response->getStatusCode(),
                $body,
            );
        }
    }

    private function processRequestException(RequestException $e): ClientException
    {
        $response = $e->getResponse();
        if ($response === null) {
            return new $this->exceptionClass(trim($e->getMessage()), 0, $e, null, null);
        }

        $statusCode = $response->getStatusCode();
        $body = (string) $response->getBody();

        $message = ($this->errorMessageResolver)($body, $statusCode);
        return new $this->exceptionClass($message ?? trim($e->getMessage()), $statusCode, $e, $statusCode, $body);
    }
}
