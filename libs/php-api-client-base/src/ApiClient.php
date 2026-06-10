<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Keboola\ApiClientBase\Auth\AuthenticatingHttpClient;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Keboola\ApiClientBase\Exception\ClientException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Component\HttpClient\RetryableHttpClient;
use Symfony\Contracts\HttpClient\Exception\ExceptionInterface;
use Symfony\Contracts\HttpClient\Exception\HttpExceptionInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Contracts\HttpClient\ResponseInterface;
use Throwable;

class ApiClient
{
    private readonly HttpClientInterface $httpClient;
    private readonly ErrorMessageResolverInterface $errorMessageResolver;

    /**
     * @param non-empty-string|null $baseUrl
     * @param list<int> $retryableStatusCodes Non-5xx status codes to also retry (e.g. [429]).
     */
    public function __construct(
        ?string $baseUrl,
        RequestAuthenticatorInterface $authenticator,
        ?ApiClientOptions $options = null,
        ?ErrorMessageResolverInterface $errorMessageResolver = null,
        array $retryableStatusCodes = [],
    ) {
        $options ??= new ApiClientOptions();
        $this->errorMessageResolver = $errorMessageResolver ?? new DefaultErrorMessageResolver();
        $logger = $options->logger ?? new NullLogger();

        // Symfony resolves relative request paths against base_uri; a trailing slash on the
        // base + slash-less relative paths gives predictable joins (matches the Guzzle setup).
        $innerClient = $options->httpClient ?? HttpClient::create([
            'base_uri' => $baseUrl === null ? null : rtrim($baseUrl, '/') . '/',
            'headers' => [
                'User-Agent' => $options->userAgent,
            ],
            'timeout' => $options->connectTimeout,
            'max_duration' => $options->requestTimeout,
        ]);

        // Wrap with auth FIRST, then retry OUTERMOST: RetryableHttpClient re-invokes the
        // auth decorator on every attempt, so file-/token-backed authenticators (e.g. the
        // projected SA token) are re-resolved per retry.
        $client = new AuthenticatingHttpClient($authenticator, $innerClient);

        if ($options->backoffMaxTries > 0) {
            $client = new RetryableHttpClient(
                $client,
                RetryStrategyFactory::create($retryableStatusCodes),
                $options->backoffMaxTries,
                $logger,
            );
        }

        $this->httpClient = $client;
    }

    /**
     * @param array<string, mixed> $options
     */
    public function sendRequest(string $method, string $path, array $options = []): void
    {
        // Lazy: only the status is awaited (which still drives retries); the body is never
        // buffered for a successful void request, and is read only to build an error message.
        $this->throwOnError($this->doSendRequest($method, $path, $options));
    }

    /**
     * @template T of ResponseModelInterface
     * @param class-string<T> $responseClass
     * @param array<string, mixed> $options
     * @return ($isList is true ? list<T> : T)
     */
    public function sendRequestAndMapResponse(
        string $method,
        string $path,
        string $responseClass,
        array $options = [],
        bool $isList = false,
    ) {
        $response = $this->doSendRequest($method, $path, $options);

        try {
            // toArray() awaits + JSON-decodes the body and throws on HTTP-error or decode
            // failure (this is also what drives RetryableHttpClient's retries).
            $data = $response->toArray();
        } catch (ExceptionInterface $e) {
            throw $this->processException($e);
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
     * Issues the request lazily — Symfony does not send/await until the response is consumed,
     * so the caller decides what to await (status only, or the full body).
     *
     * @param array<string, mixed> $options
     */
    private function doSendRequest(string $method, string $path, array $options): ResponseInterface
    {
        try {
            return $this->httpClient->request($method, $path, $options);
        } catch (ExceptionInterface $e) {
            throw $this->processException($e);
        }
    }

    /**
     * Awaits the response status (not the body) and throws on HTTP error, reading the body
     * only then — so a successful void request never buffers its body.
     */
    private function throwOnError(ResponseInterface $response): void
    {
        try {
            $statusCode = $response->getStatusCode();
        } catch (ExceptionInterface $e) {
            throw $this->processException($e);
        }

        if ($statusCode < 400) {
            return;
        }

        throw $this->errorFromResponse($response, $statusCode);
    }

    private function processException(ExceptionInterface $e): ClientException
    {
        if (!$e instanceof HttpExceptionInterface) {
            // Transport or decoding error: no HTTP response to inspect.
            return new ClientException(trim($e->getMessage()), 0, $e);
        }

        return $this->errorFromResponse($e->getResponse(), $e->getResponse()->getStatusCode(), $e);
    }

    /**
     * Builds a ClientException from an error response: the body (read non-throwing) goes to the
     * error-message resolver, falling back to the transport message or a generic one.
     */
    private function errorFromResponse(
        ResponseInterface $response,
        int $statusCode,
        ?Throwable $previous = null,
    ): ClientException {
        $body = $response->getContent(throw: false);
        $message = ($this->errorMessageResolver)($body, $statusCode);
        $fallback = $previous !== null
            ? trim($previous->getMessage())
            : sprintf('Request failed with HTTP %d', $statusCode);

        return new ClientException($message ?? $fallback, $statusCode, $previous);
    }
}
