<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use JsonException;
use Keboola\AzureApiClient\Exception\ClientException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class ApiClient
{
    public function __construct(
        private readonly GuzzleClient $guzzleClient
    ) {
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
            return $this->guzzleClient->send($request, $options);
        } catch (GuzzleException $e) {
            if ($e instanceof RequestException) {
                $this->handleRequestException($e);
            }

            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    private function handleRequestException(RequestException $e): void
    {
        $response = $e->getResponse();

        if ($response === null) {
            return;
        }

        try {
            $data = Json::decodeArray($response->getBody()->getContents());
        } catch (JsonException $e2) {
            // throw the original one, we don't care about e2
            throw new ClientException(trim($e->getMessage()), $response->getStatusCode(), $e);
        }

        $error = is_array($data['error'] ?? null) ? $data['error'] : $data;

        if (!empty($error['message']) && !empty($error['code'])) {
            throw new ClientException(
                trim($error['code'] . ': ' . $error['message']),
                $response->getStatusCode(),
                $e
            );
        }
    }
}
