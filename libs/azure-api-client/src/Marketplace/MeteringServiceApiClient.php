<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\Authentication\Authenticator\AuthenticatorInterface;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\Marketplace\Model\ReportUsageEventsBatchResult;
use Keboola\AzureApiClient\Marketplace\Model\UsageEvent;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;
use Psr\Log\LoggerInterface;

class MeteringServiceApiClient
{
    private ApiClient $apiClient;

    /**
     * @param int<0, max>|null $backoffMaxTries
     */
    public function __construct(
        ?int $backoffMaxTries = null,
        ?AuthenticatorInterface $authenticator = null,
        ?callable $requestHandler = null,
        ?LoggerInterface $logger = null,
    ) {
        $this->apiClient = new ApiClient(
            baseUrl: 'https://marketplaceapi.microsoft.com/api/',
            backoffMaxTries: $backoffMaxTries,
            authenticator: $authenticator,
            requestHandler: $requestHandler,
            logger: $logger,
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
