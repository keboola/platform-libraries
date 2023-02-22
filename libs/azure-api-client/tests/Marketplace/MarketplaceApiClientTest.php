<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace;

use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientFactory\AuthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\Authentication\AuthenticatorInterface;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Keboola\AzureApiClient\Marketplace\Model\ActivateSubscriptionRequest;
use Keboola\AzureApiClient\Marketplace\Model\ResolveSubscriptionResult;
use Keboola\AzureApiClient\Marketplace\Model\Subscription;
use Keboola\AzureApiClient\Marketplace\OperationStatus;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\Constraint\Callback;
use PHPUnit\Framework\TestCase;

class MarketplaceApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testCreateClient(): void
    {
        $authenticator = $this->createMock(AuthenticatorInterface::class);

        $client = MarketplaceApiClient::create($authenticator);

        self::assertInstanceOf(ApiClient::class, self::getPrivatePropertyValue($client, 'apiClient'));
    }

    public function testResolveSubscription(): void
    {
        $apiResponse = ResolveSubscriptionResult::fromResponseData([
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
        ]);

        $azureApiClient = $this->createMock(ApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequestAndMapResponse')
            ->with(self::checkRequestEquals(
                'POST',
                '/api/saas/subscriptions/resolve?api-version=2018-08-31',
                [
                    'x-ms-marketplace-token' => ['marketplace-token'],
                ]
            ))
            ->willReturn($apiResponse)
        ;

        $client = new MarketplaceApiClient($azureApiClient);
        $result = $client->resolveSubscription('marketplace-token');

        self::assertSame($apiResponse, $result);
    }

    public function testGetSubscription(): void
    {
        $apiResponse = Subscription::fromResponseData([
            'id' => 'subscription id',
            'publisherId' => 'publisher-id',
            'offerId' => 'offer-id',
            'planId' => 'plan-id',
            'name' => 'subscription-name',
            'quantity' => 1,
            'saasSubscriptionStatus' => 'status',
        ]);

        $azureApiClient = $this->createMock(ApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequestAndMapResponse')
            ->with(self::checkRequestEquals(
                'GET',
                '/api/saas/subscriptions/subscription+id?api-version=2018-08-31',
            ))
            ->willReturn($apiResponse)
        ;

        $client = new MarketplaceApiClient($azureApiClient);
        $result = $client->getSubscription('subscription id');

        self::assertSame($apiResponse, $result);
    }

    public function testActivateSubscription(): void
    {
        $azureApiClient = $this->createMock(ApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequest')
            ->with(self::checkRequestEquals(
                'POST',
                '/api/saas/subscriptions/subscription+id/activate?api-version=2018-08-31',
                [
                    'Content-Type' => ['application/json'],
                ],
                Json::encodeArray([
                    'planId' => 'plan-id',
                    'quantity' => 1,
                ]),
            ))
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
        $azureApiClient = $this->createMock(ApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequest')
            ->with(self::checkRequestEquals(
                'PATCH',
                '/api/saas/subscriptions/subscription+id/operations/operation+id?api-version=2018-08-31',
                [
                    'Content-Type' => ['application/json'],
                ],
                Json::encodeArray([
                    'status' => 'Success',
                ]),
            ))
        ;

        $client = new MarketplaceApiClient($azureApiClient);
        $client->updateOperationStatus(
            'subscription id',
            'operation id',
            OperationStatus::SUCCESS,
        );
    }

    /**
     * @return Callback<Request>
     */
    private static function checkRequestEquals(
        string $method,
        string $uri,
        array $headers = [],
        ?string $body = null
    ): Callback {
        return self::callback(function (Request $request) use ($method, $uri, $headers, $body) {
            self::assertSame($method, $request->getMethod());
            self::assertSame($uri, $request->getUri()->__toString());
            self::assertSame($headers, $request->getHeaders());
            self::assertSame($body ?? '', $request->getBody()->getContents());
            return true;
        });
    }
}
