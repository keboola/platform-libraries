<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\ServiceBus;

use DateTimeImmutable;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\SASTokenAuthenticatorFactory;
use Keboola\AzureApiClient\Json;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusBrokerMessageRequest;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use Keboola\AzureApiClient\Tests\ReflectionPropertyAccessTestCase;
use PHPUnit\Framework\TestCase;

class ServiceBusClientTest extends TestCase
{
    use ReflectionPropertyAccessTestCase;

    private function assertAuthorization(mixed $request): void
    {
        self::assertTrue($request->hasHeader('Authorization'));
        self::assertStringStartsWith('SharedAccessSignature sig=', $request->getHeaderLine('Authorization'));
    }

    private function getClient(HandlerStack $requestHandler): ServiceBusApiClient
    {
        $endpoint = 'https://<namespace>.servicebus.windows.net:443/';
        $client = new ServiceBusApiClient(
            serviceBusEndpoint: $endpoint,
            configuration: new ApiClientConfiguration(
                authenticator: new SASTokenAuthenticatorFactory(
                    url: $endpoint,
                    sharedAccessKeyName: 'RootManageSharedAccessKey',
                    sharedAccessKey: '<sharedAccessKey>',
                ),
                requestHandler: $requestHandler(...),
            )
        );
        return $client;
    }

    public function testPeakMessage(): void
    {
        $requestsHistory = [];
        $requestHandler = HandlerStack::create(new MockHandler([
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                    'BrokerProperties' => '{"DeliveryCount":1,"EnqueuedSequenceNumber":0,"EnqueuedTimeUtc":"Wed, 02 Jul 2014 01:32:27 GMT","Label":"M1","LockToken":"7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547","LockedUntilUtc":"Wed, 02 Jul 2014 01:33:27 GMT","MessageId":"31907572164743c38741631acd554d6f","SequenceNumber":2,"State":"Active","TimeToLive":10}',
                    'Location' => 'https://your-namespace.servicebus.windows.net/httpclientsamplequeue/messages/2/7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547',
                ],
                Json::encodeArray([
                    'message' => 'test',
                ])
            ),
        ]));
        $requestHandler->push(Middleware::history($requestsHistory));

        $client = $this->getClient($requestHandler);
        $message = $client->peakMessage('testQueue');

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://<namespace>.servicebus.windows.net/testQueue/messages/head?timeout=10', $request->getUri()->__toString());

        $this->assertAuthorization($request);

        self::assertSame('31907572164743c38741631acd554d6f', $message->id);
        self::assertSame('{"message":"test"}', $message->body);
        self::assertSame('https://your-namespace.servicebus.windows.net/httpclientsamplequeue/messages/2/7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547', $message->lockLocation);
        self::assertSame(['message' => 'test'], $message->getJsonBody());
    }

    public function testPeakMessageNoMessage(): void
    {
        $requestsHistory = [];
        $requestHandler = HandlerStack::create(new MockHandler([
            new Response(
                204,
                [
                    'Content-Type' => 'application/json',
                    'BrokerProperties' => '{"DeliveryCount":1,"EnqueuedSequenceNumber":0,"EnqueuedTimeUtc":"Wed, 02 Jul 2014 01:32:27 GMT","Label":"M1","LockToken":"7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547","LockedUntilUtc":"Wed, 02 Jul 2014 01:33:27 GMT","MessageId":"31907572164743c38741631acd554d6f","SequenceNumber":2,"State":"Active","TimeToLive":10}',
                    'Location' => 'https://your-namespace.servicebus.windows.net/httpclientsamplequeue/messages/2/7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547',
                ],
                null
            ),
        ]));
        $requestHandler->push(Middleware::history($requestsHistory));

        $client = $this->getClient($requestHandler);
        $message = $client->peakMessage('testQueue');
        self::assertNull($message);

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://<namespace>.servicebus.windows.net/testQueue/messages/head?timeout=10', $request->getUri()->__toString());

        $this->assertAuthorization($request);
    }

    public function testSendMessage(): void
    {
        $requestsHistory = [];
        $requestHandler = HandlerStack::create(new MockHandler([
            new Response(
                201,
            ),
        ]));
        $requestHandler->push(Middleware::history($requestsHistory));

        $client = $this->getClient($requestHandler);
        $client->sendMessage(
            'testQueue',
            new ServiceBusBrokerMessageRequest(
                id: '123',
                body: Json::encodeArray([
                    'message' => 'test',
                ]),
                contentType: 'application/json',
            ),
            5
        );

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://<namespace>.servicebus.windows.net/testQueue/messages', $request->getUri()->__toString());
        $this->assertAuthorization($request);

        self::assertTrue($request->hasHeader('Content-Type'));
        self::assertSame('application/json', $request->getHeaderLine('Content-Type'));
        self::assertTrue($request->hasHeader('BrokerProperties'));
        $props = Json::decodeArray($request->getHeaderLine('BrokerProperties'));
        self::assertArrayHasKey('MessageId', $props);
        self::assertSame('123', $props['MessageId']);
        self::assertArrayHasKey('ScheduledEnqueueTimeUtc', $props);
        self::assertEqualsWithDelta(
            (new DateTimeImmutable('now'))->getTimestamp(),
            (new DateTimeImmutable($props['ScheduledEnqueueTimeUtc']))->getTimestamp(),
            6 // delay of 5 seconds + 1 second for test execution
        );

        $body = json_decode($request->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
        self::assertSame(['message' => 'test'], $body);
    }

    public function testDeleteMessage(): void
    {
        $requestsHistory = [];
        $requestHandler = HandlerStack::create(new MockHandler([
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                ],
                '0'
            ),
        ]));
        $requestHandler->push(Middleware::history($requestsHistory));

        $client = $this->getClient($requestHandler);
        $client->deleteMessage('https://your-namespace.servicebus.windows.net/httpclientsamplequeue/messages/2/7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547');

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('DELETE', $request->getMethod());
        self::assertSame('https://<namespace>.servicebus.windows.net/httpclientsamplequeue/messages/2/7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547', $request->getUri()->__toString());

        $this->assertAuthorization($request);
    }
}
