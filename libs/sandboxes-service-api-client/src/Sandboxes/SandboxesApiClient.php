<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes;

use GuzzleHttp\Psr7\Request;
use Keboola\SandboxesServiceApiClient\ApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Keboola\SandboxesServiceApiClient\Json;

class SandboxesApiClient
{
    private ApiClient $apiClient;

    public function __construct(ApiClientConfiguration $configuration)
    {
        $this->apiClient = new ApiClient($configuration);
    }

    public function createSandbox(array $payload): array
    {
        return $this->apiClient->sendRequestAndDecodeResponse(
            new Request(
                'POST',
                '/sandboxes',
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray($payload),
            ),
        );
    }

    public function deleteSandbox(string $sandboxId): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'DELETE',
                sprintf('/sandboxes/%s', $sandboxId),
            ),
        );
    }

    public function updateSandbox(string $sandboxId, array $array): array
    {
        return $this->apiClient->sendRequestAndDecodeResponse(
            new Request(
                'PATCH',
                sprintf('/sandboxes/%s', $sandboxId),
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray($array),
            ),
        );
    }
}
