<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace;

use DateTimeImmutable;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\Authentication\Authenticator\StaticTokenAuthenticator;
use Keboola\AzureApiClient\Authentication\AuthorizationHeaderResolver;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\Marketplace\MeteringServiceApiClient;
use Keboola\AzureApiClient\Marketplace\Model\UsageEvent;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;
use PHPUnit\Framework\TestCase;

class MeteringServiceApiClientTest extends TestCase
{


    public function testReportUsageEventsBatch(): void
    {
        $apiResponse = [
            'result' => [
                [
                    'usageEventId' => 'event-1',
                    'status' => 'Accepted',
                    'messageTime' => '2023-01-02T14:00:00Z',
                    'resourceId' => 'resource-1',
                    'planId' => 'plan-1',
                    'dimension' => 'dim-1',
                    'quantity' => 1.0,
                    'effectiveStartTime' => '2023-01-01T12:00:00Z',
                ],
                [
                    'usageEventId' => 'event-1',
                    'status' => 'Duplicate',
                    'error' => [
                        'code' => 'err-code',
                        'message' => 'err-message',
                    ],
                    'messageTime' => '2023-01-02T14:00:00Z',
                    'resourceId' => 'resource-1',
                    'planId' => 'plan-1',
                    'dimension' => 'dim-1',
                    'quantity' => 1.0,
                    'effectiveStartTime' => '2023-01-01T12:00:00Z',
                ],
            ],
        ];

        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                Json::encodeArray($apiResponse)
            ),
        ]);

        $client = new MeteringServiceApiClient(
            authenticator: new StaticTokenAuthenticator(
                'my-token',
                AuthorizationHeaderResolver::class
            ),
            requestHandler: $requestHandler,
        );
        $result = $client->reportUsageEventsBatch([
            new UsageEvent(
                'resource-1',
                'plan-1',
                'dim-1',
                1.0,
                new DateTimeImmutable('2023-01-01T12:00:00Z'),
            ),
            new UsageEvent(
                'resource-2',
                'plan-2',
                'dim-2',
                2.5,
                new DateTimeImmutable('2023-01-02T12:00:00Z'),
            ),
        ]);

        self::assertEquals([
            UsageEventResult::fromResponseData($apiResponse['result'][0]),
            UsageEventResult::fromResponseData($apiResponse['result'][1]),
        ], $result);

        self::assertCount(1, $requestsHistory);
        self::assertRequestEquals(
            'POST',
            'https://marketplaceapi.microsoft.com/api/batchUsageEvent?api-version=2018-08-31',
            [
                'Authorization' => 'Bearer my-token',
                'Content-Type' => 'application/json',
            ],
            Json::encodeArray([
                'request' => [
                    [
                        'resourceId' => 'resource-1',
                        'planId' => 'plan-1',
                        'dimension' => 'dim-1',
                        'quantity' => 1.0,
                        'effectiveStartTime' => '2023-01-01T12:00:00+00:00',
                    ],
                    [
                        'resourceId' => 'resource-2',
                        'planId' => 'plan-2',
                        'dimension' => 'dim-2',
                        'quantity' => 2.5,
                        'effectiveStartTime' => '2023-01-02T12:00:00+00:00',
                    ],
                ],
            ]),
            $requestsHistory[0]['request'],
        );
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param list<Response>                                    $responses
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
