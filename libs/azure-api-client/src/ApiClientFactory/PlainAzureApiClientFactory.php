<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\ApiClientFactory;

use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\GuzzleClientFactory;

class PlainAzureApiClientFactory
{
    public function __construct(
        private readonly GuzzleClientFactory $guzzleClientFactory,
    ) {
    }

    public function createClient(string $baseUrl, array $options = []): ApiClient
    {
        $guzzleClient = $this->guzzleClientFactory->getClient($baseUrl, $options);
        return new ApiClient($guzzleClient);
    }
}
