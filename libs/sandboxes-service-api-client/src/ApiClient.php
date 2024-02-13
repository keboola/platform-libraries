<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use JsonException;
use Keboola\SandboxesServiceApiClient\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;
use Webmozart\Assert\Assert;

class ApiClient
{
    private readonly HandlerStack $requestHandlerStack;
    private readonly GuzzleClient $httpClient;

    /**
     * @param non-empty-string|null $baseUrl
     */
    public function __construct(
        ?string $baseUrl = null,
        ?ApiClientConfiguration $configuration = null,
    ) {
        Assert::nullOrMinLength($baseUrl, 1);
        $configuration ??= new ApiClientConfiguration();

        $this->requestHandlerStack = HandlerStack::create($configuration->requestHandler);
        $this->requestHandlerStack->remove('auth');
        if ($configuration->authenticator !== null) {
            $this->requestHandlerStack->push(Middleware::mapRequest($configuration->authenticator), 'auth');
        }

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
            'base_uri' => $baseUrl,
            'handler' => $this->requestHandlerStack,
            'headers' => [
                'User-Agent' => $configuration->userAgent,
            ],
            'connect_timeout' => 10,
            'timeout' => 120,
        ]);
    }

    /**
     * @template TResponseClass of ResponseModelInterface
     * @param class-string<TResponseClass> $responseClass
     * @return ($isList is true ? list<TResponseClass> : TResponseClass)
     */
    public function sendRequestAndMapResponse(
        RequestInterface $request,
        string $responseClass,
        array $options = [],
        bool $isList = false,
    ) {
        $response = $this->doSendRequest($request, $options);

        try {
            $responseData = Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException $e) {
            throw new ClientException('Response is not a valid JSON: ' . $e->getMessage(), $e->getCode(), $e);
        }

        try {
            if ($isList) {
                return array_map($responseClass::fromResponseData(...), $responseData);
            } else {
                return $responseClass::fromResponseData($responseData);
            }
        } catch (Throwable $e) {
            throw new ClientException('Failed to map response data: ' . $e->getMessage(), 0, $e);
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

        $error = is_array($data['error'] ?? null) ? $data['error'] : $data;
        if (empty($error['message']) || empty($error['code'])) {
            return null;
        }

        return new ClientException(
            trim($error['code'] . ': ' . $error['message']),
            $response->getStatusCode(),
            $e,
        );
    }
}
