<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes;

use GuzzleHttp\Psr7\Request;
use Keboola\SandboxesServiceApiClient\ApiClient;
use Keboola\SandboxesServiceApiClient\ApiClientConfiguration;
use Keboola\SandboxesServiceApiClient\Json;
use Keboola\SandboxesServiceApiClient\Sandboxes\Model\CreateSandboxResult;

class SandboxesClient
{
    private ApiClient $apiClient;

    public function __construct(
        ?string $baseUrl = null,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient(
            $baseUrl,
            $configuration,
        );
    }

    public function createSandbox(CreateSandboxPayload $payload): ResolveSubscriptionResult
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                '/sandboxes',
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray((array) $payload),
            ),
            CreateSandboxResult::class,
        );
    }

}
