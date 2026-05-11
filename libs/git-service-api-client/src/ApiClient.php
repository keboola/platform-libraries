<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use JsonException;
use Keboola\GitServiceApiClient\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use SensitiveParameter;
use Throwable;

class ApiClient
{
    private const USER_AGENT = 'Keboola Git Service PHP Client';

    private readonly GuzzleClient $httpClient;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $token
     */
    public function __construct(
        string $baseUrl,
        #[SensitiveParameter] string $token,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $configuration ??= new ApiClientConfiguration();

        $stack = $configuration->requestHandler instanceof HandlerStack
            ? $configuration->requestHandler
            : HandlerStack::create($configuration->requestHandler);
        $stack->push(Middleware::mapRequest(
            fn (RequestInterface $request): RequestInterface
                => $request->withHeader('X-KBC-ManageApiToken', $token),
        ));

        if ($configuration->backoffMaxTries > 0) {
            $stack->push(Middleware::retry(new RetryDecider(
                $configuration->backoffMaxTries,
                $configuration->logger,
            )));
        }

        $stack->push(Middleware::log(
            $configuration->logger,
            new MessageFormatter('[git-service-api] {method} {uri} : {code} {res_header_Content-Length}'),
        ));

        $userAgent = self::USER_AGENT;
        if ($configuration->userAgent !== null) {
            $userAgent .= ' - ' . $configuration->userAgent;
        }

        $this->httpClient = new GuzzleClient([
            'base_uri' => rtrim($baseUrl, '/') . '/',
            'handler' => $stack,
            'headers' => [
                'User-Agent' => $userAgent,
                'Content-Type' => 'application/json',
            ],
            'connect_timeout' => 10,
            'timeout' => 120,
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
                return array_values(array_map($responseClass::fromResponseData(...), $data));
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
            throw $this->processRequestException($e) ?? new ClientException(
                $e->getMessage(),
                $e->getResponse()?->getStatusCode() ?? 0,
                $e,
            );
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), 0, $e);
        }
    }

    private function processRequestException(RequestException $e): ?ClientException
    {
        $response = $e->getResponse();
        if ($response === null) {
            return null;
        }

        try {
            $data = Json::decodeArray((string) $response->getBody());
        } catch (JsonException) {
            return new ClientException(trim($e->getMessage()), $response->getStatusCode(), $e);
        }

        $code = $data['code'] ?? null;
        $error = $data['error'] ?? null;
        if (!is_string($code) || !is_string($error) || $code === '' || $error === '') {
            return new ClientException(trim($e->getMessage()), $response->getStatusCode(), $e);
        }

        return new ClientException(
            trim($code . ': ' . $error),
            $response->getStatusCode(),
            $e,
        );
    }
}
