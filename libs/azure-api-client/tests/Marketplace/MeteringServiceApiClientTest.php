<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace;

use DateTimeImmutable;
use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\ApiClient;
use Keboola\AzureApiClient\ApiClientFactory\AuthenticatedAzureApiClientFactory;
use Keboola\AzureApiClient\Marketplace\MeteringServiceApiClient;
use Keboola\AzureApiClient\Marketplace\Model\ReportUsageEventsBatchResult;
use Keboola\AzureApiClient\Marketplace\Model\UsageEvent;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\Constraint\Callback;
use PHPUnit\Framework\TestCase;

class MeteringServiceApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testCreateClient(): void
    {
        $azureApiClient = $this->createMock(ApiClient::class);

        $clientFactory = $this->createMock(AuthenticatedAzureApiClientFactory::class);
        $clientFactory->expects(self::once())
            ->method('createClient')
            ->with('https://marketplaceapi.microsoft.com/api/', '20e940b3-4c77-4b0b-9a53-9e16a1b010a7')
            ->willReturn($azureApiClient)
        ;

        $client = MeteringServiceApiClient::create($clientFactory);

        self::assertSame($azureApiClient, self::getPrivatePropertyValue($client, 'apiClient'));
    }

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

        $azureApiClient = $this->createMock(ApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequestAndMapResponse')
            ->with(self::checkRequestEquals(
                'POST',
                'batchUsageEvent?api-version=2018-08-31',
                [
                    'Content-Type' => ['application/json'],
                ],
                (string) json_encode([
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
            ))
            ->willReturn(ReportUsageEventsBatchResult::fromResponseData($apiResponse))
        ;

        $client = new MeteringServiceApiClient($azureApiClient);
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
