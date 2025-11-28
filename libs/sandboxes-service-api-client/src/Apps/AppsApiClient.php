<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Apps;

use GuzzleHttp\Psr7\Request;
use Keboola\SandboxesServiceApiClient\ApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Keboola\SandboxesServiceApiClient\Json;

class AppsApiClient
{
    private ApiClient $apiClient;

    public function __construct(ApiClientConfiguration $configuration)
    {
        $this->apiClient = new ApiClient($configuration);
    }

    /**
     * @param int|null $offset
     * @param int|null $limit
     * @return array<App>
     */
    public function listApps(?int $offset = null, ?int $limit = null): array
    {
        $queryParams = [];
        if ($offset !== null) {
            $queryParams['offset'] = (string) $offset;
        }
        if ($limit !== null) {
            $queryParams['limit'] = (string) $limit;
        }

        $uri = '/apps';
        if (!empty($queryParams)) {
            $uri .= '?' . http_build_query($queryParams);
        }

        $responseData = $this->apiClient->sendRequestAndDecodeResponse(
            new Request('GET', $uri),
        );

        return array_map(fn(array $appData) => App::fromArray($appData), $responseData);
    }

    public function getApp(string $appId): App
    {
        $responseData = $this->apiClient->sendRequestAndDecodeResponse(
            new Request(
                'GET',
                sprintf('/apps/%s', $appId),
            ),
        );

        return App::fromArray($responseData);
    }

    /**
     * @param string $appId
     * @param array<string, mixed> $payload
     */
    public function patchApp(string $appId, array $payload): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'PATCH',
                sprintf('/apps/%s', $appId),
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray($payload),
            ),
        );
    }
}
