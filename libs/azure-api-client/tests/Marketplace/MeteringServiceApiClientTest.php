<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Marketplace;

use DateTimeImmutable;
use Keboola\AzureApiClient\AzureApiClient;
use Keboola\AzureApiClient\AzureApiClientFactory;
use Keboola\AzureApiClient\Marketplace\MeteringServiceApiClient;
use Keboola\AzureApiClient\Marketplace\Model\UsageEvent;
use Keboola\AzureApiClient\Marketplace\Model\UsageEventResult;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\TestCase;

class MeteringServiceApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testCreateClient(): void
    {
        $azureApiClient = $this->createMock(AzureApiClient::class);

        $clientFactory = $this->createMock(AzureApiClientFactory::class);
        $clientFactory->expects(self::once())
            ->method('getClient')
            ->with('https://marketplaceapi.microsoft.com/api/', '20e940b3-4c77-4b0b-9a53-9e16a1b010a7')
            ->willReturn($azureApiClient)
        ;

        $client = MeteringServiceApiClient::create($clientFactory);

        self::assertSame($azureApiClient, self::getPrivatePropertyValue($client, 'azureApiClient'));
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
            ]
        ];

        $azureApiClient = $this->createMock(AzureApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequest')
            ->with(
                'POST',
                'batchUsageEvent?api-version=2018-08-31',
                [
                    'Content-Type' => 'application/json',
                ],
                json_encode([
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
                    ]
                ])
            )
            ->willReturn($apiResponse)
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
}
