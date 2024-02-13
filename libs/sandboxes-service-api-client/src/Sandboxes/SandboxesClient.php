<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes;

use GuzzleHttp\Psr7\Request;
use Keboola\SandboxesServiceApiClient\ApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Keboola\SandboxesServiceApiClient\Json;

class SandboxesClient
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
}
