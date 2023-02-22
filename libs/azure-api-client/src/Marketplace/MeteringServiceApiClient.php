<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientFactory\AuthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\Marketplace\Model\ReportUsageEventsBatchResult;
use Keboola\AzureApiClient\Marketplace\Model\UsageEvent;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;

class MeteringServiceApiClient
{
    public function __construct(
        private readonly ApiClient $apiClient,
    ) {
    }

    public static function create(AuthenticatorInterface $authenticator): self
    {
        $apiClient = (new AuthenticatedAzureApiClientFactory($authenticator))->createClient(
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
