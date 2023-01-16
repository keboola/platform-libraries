<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace;

use Keboola\AzureApiClient\AzureApiClient;
use Keboola\AzureApiClient\AzureApiClientFactory;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Keboola\AzureApiClient\Marketplace\Model\ActivateSubscriptionRequest;
use Keboola\AzureApiClient\Marketplace\Model\ResolveSubscriptionResult;
use Keboola\AzureApiClient\Marketplace\Model\Subscription;
use Keboola\AzureApiClient\Marketplace\OperationStatus;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\TestCase;

class MarketplaceApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testCreateClient(): void
    {
        $azureApiClient = $this->createMock(AzureApiClient::class);

        $clientFactory = $this->createMock(AzureApiClientFactory::class);
        $clientFactory->expects(self::once())
            ->method('getClient')
            ->with('https://marketplaceapi.microsoft.com', '20e940b3-4c77-4b0b-9a53-9e16a1b010a7')
            ->willReturn($azureApiClient)
        ;

        $client = MarketplaceApiClient::create($clientFactory);

        self::assertSame($azureApiClient, self::getPrivatePropertyValue($client, 'azureApiClient'));
    }

    public function testResolveSubscription(): void
    {
        $apiResponse = [
            'id' => 'subscription-id',
            'subscriptionName' => 'subscription-name',
            'offerId' => 'offer-id',
            'planId' => 'plan-id',
            'subscription' => [
                'id' => 'subscription-id',
                'publisherId' => 'publisher-id',
                'offerId' => 'offer-id',
                'planId' => 'plan-id',
                'name' => 'subscription-name',
                'quantity' => 1,
                'saasSubscriptionStatus' => 'status',
            ],
        ];

        $azureApiClient = $this->createMock(AzureApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequest')
            ->with(
                'POST',
                '/api/saas/subscriptions/resolve?api-version=2018-08-31',
                [
                    'x-ms-marketplace-token' => 'marketplace-token',
                ]
            )
            ->willReturn($apiResponse)
        ;

        $client = new MarketplaceApiClient($azureApiClient);
        $result = $client->resolveSubscription('marketplace-token');

        self::assertEquals(ResolveSubscriptionResult::fromResponseData($apiResponse), $result);
    }

    public function testGetSubscription(): void
    {
        $apiResponse = [
            'id' => 'subscription id',
            'publisherId' => 'publisher-id',
            'offerId' => 'offer-id',
            'planId' => 'plan-id',
            'name' => 'subscription-name',
            'quantity' => 1,
            'saasSubscriptionStatus' => 'status',
        ];

        $azureApiClient = $this->createMock(AzureApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequest')
            ->with(
                'GET',
                '/api/saas/subscriptions/subscription+id?api-version=2018-08-31',
            )
            ->willReturn($apiResponse)
        ;

        $client = new MarketplaceApiClient($azureApiClient);
        $result = $client->getSubscription('subscription id');

        self::assertEquals(Subscription::fromResponseData($apiResponse), $result);
    }

    public function testActivateSubscription(): void
    {
        $azureApiClient = $this->createMock(AzureApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequest')
            ->with(
                'POST',
                '/api/saas/subscriptions/subscription+id/activate?api-version=2018-08-31',
                [
                    'Content-Type' => 'application/json',
                ],
                json_encode([
                    'planId' => 'plan-id',
                    'quantity' => 1,
                ]),
                false,
            )
        ;

        $client = new MarketplaceApiClient($azureApiClient);
        $client->activateSubscription(new ActivateSubscriptionRequest(
            'subscription id',
            'plan-id',
            1,
        ));
    }

    public function testUpdateOperationStatus(): void
    {
        $azureApiClient = $this->createMock(AzureApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequest')
            ->with(
                'PATCH',
                '/api/saas/subscriptions/subscription+id/operations/operation+id?api-version=2018-08-31',
                [
                    'Content-Type' => 'application/json',
                ],
                json_encode([
                    'status' => 'Success',
                ]),
                false,
            )
        ;

        $client = new MarketplaceApiClient($azureApiClient);
        $client->updateOperationStatus(
            'subscription id',
            'operation id',
            OperationStatus::SUCCESS,
        );
    }
}
