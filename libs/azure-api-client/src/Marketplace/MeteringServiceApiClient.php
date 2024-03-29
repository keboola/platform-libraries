<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\Marketplace\Model\ReportUsageEventsBatchResult;
use Keboola\AzureApiClient\Marketplace\Model\UsageEvent;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;

class MeteringServiceApiClient
{
    private ApiClient $apiClient;

    public function __construct(
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient(
            'https://marketplaceapi.microsoft.com/api/',
            $configuration,
        );
        $this->apiClient->authenticate(Resources::AZURE_MARKETPLACE);
    }

    /**
     * @param list<UsageEvent> $events
     * @return list<UsageEventResult>
     */
    public function reportUsageEventsBatch(array $events): array
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'batchUsageEvent?api-version=2018-08-31',
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    'request' => $events,
                ]),
            ),
            ReportUsageEventsBatchResult::class,
        )->result;
    }
}
