<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\EventGrid;

use DateTimeImmutable;
use GuzzleHttp\Psr7\Request;
use Keboola\AzureApiClient\AzureApiClient;
use Keboola\AzureApiClient\AzureApiClientFactory;
use Keboola\AzureApiClient\EventGrid\EventGridApiClient;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\Constraint\Callback;
use PHPUnit\Framework\TestCase;

class EventGridApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testCreateClient(): void
    {
        $azureApiClient = $this->createMock(AzureApiClient::class);

        $clientFactory = $this->createMock(AzureApiClientFactory::class);
        $clientFactory->expects(self::once())
            ->method('getClient')
            ->with('https://test-eventgrid.northeurope-1.eventgrid.azure.net', 'EVENT_GRID')
            ->willReturn($azureApiClient);

        $client = EventGridApiClient::create(
            $clientFactory,
            'test-eventgrid.northeurope-1.eventgrid.azure.net'
        );

        self::assertSame($azureApiClient, self::getPrivatePropertyValue($client, 'azureApiClient'));
    }

    public function testGetSubscription(): void
    {
        $azureApiClient = $this->createMock(AzureApiClient::class);
        $azureApiClient->expects(self::once())
            ->method('sendRequest')
            ->with(self::callback(function (Request $request) {
                self::assertSame('POST', $request->getMethod());
                self::assertSame('/api/events?api-version=2018-01-01', $request->getUri()->__toString());
                self::assertSame([], $request->getHeaders());
                /** @var array<array<mixed>> $body */
                $body = json_decode($request->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
                $body = $body[0];
                self::assertSame('3e9825ed-b7db-44ea-a5c9-a1601fa43e23', $body['id']);
                self::assertSame('TestSubject', $body['subject']);
                self::assertSame([
                    'Property1' => 'Value1',
                    'Property2' => 'Value2',
                ], $body['data']);
                self::assertSame('Keboola.EventGridClient.TestEvent', $body['eventType']);
                self::assertEqualsWithDelta(
                    (new DateTimeImmutable('now'))->format('Y-m-d\TH:i:s\Z'),
                    $body['eventTime'],
                    5
                );
                self::assertSame('1.0', $body['dataVersion']);
                return true;
            }));

        $client = new EventGridApiClient($azureApiClient);
        $client->publishEvents([
            new EventGridEvent(
                '3e9825ed-b7db-44ea-a5c9-a1601fa43e23',
                'TestSubject',
                [
                    'Property1' => 'Value1',
                    'Property2' => 'Value2',
                ],
                'Keboola.EventGridClient.TestEvent'
            ),
        ]);
    }
}
