<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace;

use Keboola\AzureApiClient\AzureApiClient;
use Keboola\AzureApiClient\AzureApiClientFactory;
use Keboola\AzureApiClient\Marketplace\Model\UsageEvent;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;

class MeteringServiceApiClient
{
    public function __construct(
        private readonly AzureApiClient $azureApiClient,
    ) {
    }

    public static function create(AzureApiClientFactory $clientFactory): self
    {
        $apiClient = $clientFactory->getClient(
            'https://marketplaceapi.microsoft.com/api/',
            Resources::AZURE_MARKETPLACE
        );
        return new self($apiClient);
    }

    /**
     * @param list<UsageEvent> $events
     * @return list<UsageEventResult>
     */
    public function reportUsageEventsBatch(array $events): array
    {
        $responseData = $this->azureApiClient->sendRequest(
            'POST',
            'batchUsageEvent?api-version=2018-08-31',
            [
                'Content-Type' => 'application/json',
            ],
            json_encode([
                'request' => $events,
            ], JSON_THROW_ON_ERROR),
        );

        return array_map(UsageEventResult::fromResponseData(...), $responseData['result']);
    }
}
