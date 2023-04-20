<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\EventGrid;

use DateTimeImmutable;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\CustomHeaderAuth;
use Keboola\AzureApiClient\EventGrid\EventGridApiClient;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\TestCase;

class EventGridApiClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    public function testPublishEvent(): void
    {
        $requestsHistory = [];
        $requestHandler = HandlerStack::create(new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                null
            ),
        ]));
        $requestHandler->push(Middleware::history($requestsHistory));

        $client = new EventGridApiClient(
            'test-eventgrid.northeurope-1.eventgrid.azure.net',
            'token',
            new ApiClientConfiguration(
                authenticator: new CustomHeaderAuth(
                    header: 'aeg-sas-key',
                    value: 'token',
                ),
                requestHandler: $requestHandler(...),
            )
        );
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

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://test-eventgrid.northeurope-1.eventgrid.azure.net/api/events?api-version=2018-01-01', $request->getUri()->__toString());

        self::assertTrue($request->hasHeader('aeg-sas-key'));
        self::assertSame('token', $request->getHeaderLine('aeg-sas-key'));

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
    }
}
