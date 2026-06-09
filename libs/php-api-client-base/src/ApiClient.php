<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Closure;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use JsonException;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Keboola\ApiClientBase\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class ApiClient
{
    private readonly GuzzleClient $httpClient;
    /** @var (Closure(string, int): ?string)|null */
    private readonly ?Closure $errorMessageResolver;

    /**
     * @param non-empty-string|null $baseUrl
     * @param list<int> $retryableStatusCodes Non-5xx status codes to also retry (e.g. [429]).
     * @param (Closure(string, int): ?string)|null $errorMessageResolver
     *   Maps a (responseBody, statusCode) to an error message, or null to fall back to the default.
     */
    public function __construct(
        ?string $baseUrl,
        RequestAuthenticatorInterface $authenticator,
        ?ApiClientOptions $options = null,
        ?Closure $errorMessageResolver = null,
        array $retryableStatusCodes = [],
    ) {
        $options ??= new ApiClientOptions();
        $this->errorMessageResolver = $errorMessageResolver;

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
                $options->logger,
                $retryableStatusCodes,
            )));
        }

        $stack->push(Middleware::mapRequest($authenticator));

        $stack->push(Middleware::log(
            $options->logger,
            new MessageFormatter('{method} {uri} : {code} {res_header_Content-Length}'),
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

    public function sendRequest(RequestInterface $request): void
    {
        $this->doSendRequest($request);
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
        $response = $this->doSendRequest($request, $options);

        try {
            $data = Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException $e) {
            throw new ClientException('Response is not valid JSON: ' . $e->getMessage(), 0, $e);
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
            throw new ClientException('Failed to map response data: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * @param array<string, mixed> $options
     */
    private function doSendRequest(RequestInterface $request, array $options = []): ResponseInterface
    {
        try {
            return $this->httpClient->send($request, $options);
        } catch (RequestException $e) {
            throw $this->processRequestException($e);
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
    }

    private function processRequestException(RequestException $e): ClientException
    {
        $response = $e->getResponse();
        if ($response === null) {
            return new ClientException(trim($e->getMessage()), 0, $e);
        }

        $statusCode = $response->getStatusCode();
        $body = (string) $response->getBody();

        if ($this->errorMessageResolver !== null) {
            $message = ($this->errorMessageResolver)($body, $statusCode);
            if ($message !== null && $message !== '') {
                return new ClientException($message, $statusCode, $e);
            }
            return new ClientException(trim($e->getMessage()), $statusCode, $e);
        }

        return new ClientException($this->defaultErrorMessage($body) ?? trim($e->getMessage()), $statusCode, $e);
    }

    private function defaultErrorMessage(string $body): ?string
    {
        try {
            $data = Json::decodeArray($body);
        } catch (JsonException) {
            return null;
        }

        foreach (['error', 'message'] as $key) {
            if (isset($data[$key]) && is_string($data[$key]) && $data[$key] !== '') {
                return $data[$key];
            }
        }

        return null;
    }
}
