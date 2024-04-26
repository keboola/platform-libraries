<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient;

use GuzzleHttp\BodySummarizer;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use JsonException;
use Keboola\SandboxesServiceApiClient\Authentication\StorageTokenAuthenticator;
use Keboola\SandboxesServiceApiClient\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

class ApiClient
{
    private const MAX_HTTP_ERROR_MESSAGE_LENGTH = 1024^2;
    private readonly HandlerStack $requestHandlerStack;
    private readonly GuzzleClient $httpClient;

    public function __construct(ApiClientConfiguration $configuration)
    {
        $this->requestHandlerStack = HandlerStack::create($configuration->requestHandler);

        $this->requestHandlerStack->remove('auth');
        $this->requestHandlerStack->push(
            Middleware::mapRequest(new StorageTokenAuthenticator($configuration->storageToken)),
            'auth',
        );

        $this->requestHandlerStack->remove('http_errors');
        $this->requestHandlerStack->unshift(
            Middleware::httpErrors(new BodySummarizer(self::MAX_HTTP_ERROR_MESSAGE_LENGTH)),
            'http_errors',
        );

        if ($configuration->backoffMaxTries > 0) {
            $this->requestHandlerStack->push(Middleware::retry(new RetryDecider(
                $configuration->backoffMaxTries,
                $configuration->logger,
            )));
        }

        $this->requestHandlerStack->push(Middleware::log($configuration->logger, new MessageFormatter(
            '{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}"' .
            ' {code} {res_header_Content-Length}',
        )));

        $this->httpClient = new GuzzleClient([
            'base_uri' => $configuration->baseUrl,
            'handler' => $this->requestHandlerStack,
            'headers' => [
                'User-Agent' => $configuration->userAgent,
            ],
            'connect_timeout' => 10,
            'timeout' => 120,
        ]);
    }

    public function sendRequestAndDecodeResponse(
        RequestInterface $request,
        array $options = [],
    ): array {
        $response = $this->doSendRequest($request, $options);

        try {
            return Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException $e) {
            throw new ClientException('Response is not a valid JSON: ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    public function sendRequest(RequestInterface $request): void
    {
        $this->doSendRequest($request);
    }

    private function doSendRequest(RequestInterface $request, array $options = []): ResponseInterface
    {
        try {
            return $this->httpClient->send($request, $options);
        } catch (RequestException $e) {
            throw $this->processRequestException($e) ?? new ClientException($e->getMessage(), $e->getCode(), $e);
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    private function processRequestException(RequestException $e): ?ClientException
    {
        $response = $e->getResponse();
        if ($response === null) {
            return null;
        }

        try {
            $data = Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException) {
            // throw the original one, we don't care about e2
            return new ClientException(trim($e->getMessage()), $response->getStatusCode(), $e);
        }

        if (empty($data['error']) || empty($data['message'])) {
            return null;
        }

        return new ClientException(
            trim(sprintf('%s: %s', $data['error'], $data['message'])),
            $response->getStatusCode(),
            $e,
        );
    }
}
