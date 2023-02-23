<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\Authentication\Authenticator\StaticTokenAuthenticator;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\Marketplace\MarketplaceApiClient;
use Keboola\AzureApiClient\Marketplace\Model\ActivateSubscriptionRequest;
use Keboola\AzureApiClient\Marketplace\Model\ResolveSubscriptionResult;
use Keboola\AzureApiClient\Marketplace\Model\Subscription;
use Keboola\AzureApiClient\Marketplace\OperationStatus;
use PHPUnit\Framework\TestCase;

class MarketplaceApiClientTest extends TestCase
{
    public function testResolveSubscription(): void
    {
        $subscriptionData = [
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

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($subscriptionData)
            ),
        ]);

        $client = new MarketplaceApiClient(
            authenticator: new StaticTokenAuthenticator('my-token'),
            requestHandler: $requestHandler,
        );
        $result = $client->resolveSubscription('marketplace-token');

        self::assertEquals(
            ResolveSubscriptionResult::fromResponseData($subscriptionData),
            $result
        );

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'POST',
            'https://marketplaceapi.microsoft.com/api/saas/subscriptions/resolve?api-version=2018-08-31',
            [
                'authorization' => 'Bearer my-token',
                'x-ms-marketplace-token' => 'marketplace-token',
            ],
            null,
            $requestsHistory[0]['request'],
        );
    }

    public function testGetSubscription(): void
    {
        $subscriptionData = [
            'id' => 'subscription id',
            'publisherId' => 'publisher-id',
            'offerId' => 'offer-id',
            'planId' => 'plan-id',
            'name' => 'subscription-name',
            'quantity' => 1,
            'saasSubscriptionStatus' => 'status',
        ];

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($subscriptionData)
            ),
        ]);

        $client = new MarketplaceApiClient(
            authenticator: new StaticTokenAuthenticator('my-token'),
            requestHandler: $requestHandler,
        );
        $result = $client->getSubscription('subscription id');

        self::assertEquals(
            Subscription::fromResponseData($subscriptionData),
            $result
        );

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'GET',
            'https://marketplaceapi.microsoft.com/api/saas/subscriptions/subscription+id?api-version=2018-08-31',
            [
                'Authorization' => 'Bearer my-token',
            ],
            null,
            $requestsHistory[0]['request'],
        );
    }

    public function testActivateSubscription(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        $client = new MarketplaceApiClient(
            authenticator: new StaticTokenAuthenticator('my-token'),
            requestHandler: $requestHandler,
        );
        $client->activateSubscription(new ActivateSubscriptionRequest(
            'subscription id',
            'plan-id',
            1,
        ));

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'POST',
            // phpcs:ignore Generic.Files.LineLength
            'https://marketplaceapi.microsoft.com/api/saas/subscriptions/subscription+id/activate?api-version=2018-08-31',
            [
                'Authorization' => 'Bearer my-token',
                'Content-Type' => 'application/json',
            ],
            Json::encodeArray([
                'planId' => 'plan-id',
                'quantity' => 1,
            ]),
            $requestsHistory[0]['request'],
        );
    }

    public function testUpdateOperationStatus(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(200),
        ]);

        $client = new MarketplaceApiClient(
            authenticator: new StaticTokenAuthenticator('my-token'),
            requestHandler: $requestHandler,
        );
        $client->updateOperationStatus(
            'subscription id',
            'operation id',
            OperationStatus::SUCCESS,
        );

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'PATCH',
            // phpcs:ignore Generic.Files.LineLength
            'https://marketplaceapi.microsoft.com/api/saas/subscriptions/subscription+id/operations/operation+id?api-version=2018-08-31',
            [
                'Authorization' => 'Bearer my-token',
                'Content-Type' => 'application/json',
            ],
            Json::encodeArray([
                'status' => 'Success',
            ]),
            $requestsHistory[0]['request'],
        );
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param list<Response> $responses
     * @return HandlerStack
     */
    private static function createRequestHandler(?array &$requestsHistory, array $responses): HandlerStack
    {
        $requestsHistory = [];

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));

        return $stack;
    }

    private static function assertRequestEquals(
        string $method,
        string $uri,
        array $headers,
        ?string $body,
        Request $request,
    ): void {
        self::assertSame($method, $request->getMethod());
        self::assertSame($uri, $request->getUri()->__toString());

        foreach ($headers as $headerName => $headerValue) {
            self::assertSame($headerValue, $request->getHeaderLine($headerName));
        }

        self::assertSame($body ?? '', $request->getBody()->getContents());
    }
}
