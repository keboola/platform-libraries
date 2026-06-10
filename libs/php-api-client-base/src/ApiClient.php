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
        $this->doSendRequest($method, $path, $options)->getContent();
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
            $data = $response->toArray();
        } catch (ExceptionInterface $e) {
            // toArray() also throws on HTTP errors; those are already handled in doSendRequest's
            // getContent() check below, so reaching here means a transport/decoding failure.
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
     * Issues the request and forces evaluation so HTTP-error and transport exceptions surface
     * here (Symfony responses are lazy: getStatusCode() never throws, content access does).
     *
     * @param array<string, mixed> $options
     */
    private function doSendRequest(string $method, string $path, array $options): ResponseInterface
    {
        try {
            $response = $this->httpClient->request($method, $path, $options);
            // Buffer the body now so a non-2xx status throws here rather than lazily later.
            $response->getContent();
            return $response;
        } catch (ExceptionInterface $e) {
            throw $this->processException($e);
        }
    }

    private function processException(ExceptionInterface $e): ClientException
    {
        if (!$e instanceof HttpExceptionInterface) {
            // Transport or decoding error: no HTTP response to inspect.
            return new ClientException(trim($e->getMessage()), 0, $e);
        }

        $response = $e->getResponse();
        $statusCode = $response->getStatusCode();
        $body = $response->getContent(throw: false);

        $message = ($this->errorMessageResolver)($body, $statusCode);
        return new ClientException($message ?? trim($e->getMessage()), $statusCode, $e);
    }
}
