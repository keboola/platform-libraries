<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Marketplace;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\Marketplace\Model\ActivateSubscriptionRequest;
use Keboola\AzureApiClient\Marketplace\Model\ResolveSubscriptionResult;
use Keboola\AzureApiClient\Marketplace\Model\Subscription;

class MarketplaceApiClient
{
    private ApiClient $apiClient;

    public function __construct(
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient(
            'https://marketplaceapi.microsoft.com',
            $configuration,
        );
        $this->apiClient->authenticate(Resources::AZURE_MARKETPLACE);
    }

    public function resolveSubscription(string $marketplaceToken): ResolveSubscriptionResult
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                '/api/saas/subscriptions/resolve?api-version=2018-08-31',
                [
                    'x-ms-marketplace-token' => $marketplaceToken,
                ],
            ),
            ResolveSubscriptionResult::class,
        );
    }

    public function getSubscription(string $subscriptionId): Subscription
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                sprintf(
                    '/api/saas/subscriptions/%s?api-version=2018-08-31',
                    urlencode($subscriptionId),
                ),
            ),
            Subscription::class,
        );
    }

    public function activateSubscription(ActivateSubscriptionRequest $parameters): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'POST',
                sprintf(
                    '/api/saas/subscriptions/%s/activate?api-version=2018-08-31',
                    urlencode($parameters->subscriptionId),
                ),
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    'planId' => $parameters->planId,
                    'quantity' => $parameters->quantity,
                ]),
            ),
        );
    }

    public function updateOperationStatus(string $subscriptionId, string $operationId, OperationStatus $status): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'PATCH',
                sprintf(
                    '/api/saas/subscriptions/%s/operations/%s?api-version=2018-08-31',
                    urlencode($subscriptionId),
                    urlencode($operationId),
                ),
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray([
                    'status' => $status->value,
                ]),
            ),
        );
    }
}
