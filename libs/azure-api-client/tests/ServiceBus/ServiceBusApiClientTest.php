<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\ServiceBus;

use DateTimeImmutable;
use Generator;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\Authenticator\CustomHeaderAuth;
use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorFactoryInterface;
use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorInterface;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\Exception\InvalidResponseException;
use Keboola\AzureApiClient\ServiceBus\Exception\MessageLockLostException;
use Keboola\AzureApiClient\ServiceBus\Model\ServiceBusMessage;
use Keboola\AzureApiClient\ServiceBus\Resources;
use Keboola\AzureApiClient\ServiceBus\ServiceBusApiClient;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

class ServiceBusApiClientTest extends TestCase
{
    private const ENDPOINT = 'https://my-namespace.servicebus.windows.net:443/';
    private const LOCK_LOCATION =
        'https://my-namespace.servicebus.windows.net/queue-main/messages/2/7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547';
    // phpcs:ignore Generic.Files.LineLength
    private const BROKER_PROPERTIES = '{"DeliveryCount":2,"EnqueuedSequenceNumber":0,"EnqueuedTimeUtc":"Wed, 02 Jul 2014 01:32:27 GMT","Label":"M1","LockToken":"7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547","LockedUntilUtc":"Wed, 02 Jul 2014 01:33:27 GMT","MessageId":"31907572164743c38741631acd554d6f","SequenceNumber":2,"State":"Active","TimeToLive":10}';

    public function testAuthenticatesForServiceBusResource(): void
    {
        $authenticatorFactory = $this->createMock(RequestAuthenticatorFactoryInterface::class);
        $authenticatorFactory->expects(self::once())
            ->method('createRequestAuthenticator')
            ->with(Resources::AZURE_SERVICE_BUS)
            ->willReturn(new class implements RequestAuthenticatorInterface {
                public function __invoke(RequestInterface $request): RequestInterface
                {
                    return $request->withHeader('Authorization', 'Bearer entra-token');
                }
            });

        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(201)]);
        $client = new ServiceBusApiClient(self::ENDPOINT, new ApiClientConfiguration(
            authenticator: $authenticatorFactory,
            requestHandler: $requestHandler(...),
        ));
        $client->sendMessage('queue-main', new ServiceBusMessage('id', 'body', 'text/plain'));

        self::assertCount(1, $requestsHistory);
        self::assertSame('Bearer entra-token', $requestsHistory[0]['request']->getHeaderLine('Authorization'));
    }

    public function testSendMessage(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(201)]);
        $client = self::createClient($requestHandler);

        $client->sendMessage('queue-main', ServiceBusMessage::json('message-id', ['foo' => 'bar']));

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame(
            'https://my-namespace.servicebus.windows.net/queue-main/messages',
            (string) $request->getUri(),
        );
        self::assertSame('secret', $request->getHeaderLine('X-Auth'));
        self::assertSame('application/json', $request->getHeaderLine('Content-Type'));
        self::assertSame('{"MessageId":"message-id"}', $request->getHeaderLine('BrokerProperties'));
        self::assertSame('{"foo":"bar"}', (string) $request->getBody());
    }

    public function testSendScheduledMessage(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(201)]);
        $client = self::createClient($requestHandler);

        $client->sendMessage('topic/subscriptions/sub_1', new ServiceBusMessage(
            'message-id',
            'plain body',
            'text/plain',
            new DateTimeImmutable('2024-03-05T14:15:16+02:00'),
        ));

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame(
            'https://my-namespace.servicebus.windows.net/topic/subscriptions/sub_1/messages',
            (string) $request->getUri(),
        );
        self::assertSame('text/plain', $request->getHeaderLine('Content-Type'));
        self::assertSame(
            '{"MessageId":"message-id","ScheduledEnqueueTimeUtc":"Tue, 05 Mar 2024 12:15:16 GMT"}',
            $request->getHeaderLine('BrokerProperties'),
        );
        self::assertSame('plain body', (string) $request->getBody());
    }

    public function testPeekLockMessage(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                201,
                [
                    'Content-Type' => 'application/json',
                    'BrokerProperties' => self::BROKER_PROPERTIES,
                    'Location' => self::LOCK_LOCATION,
                ],
                '{"message":"test"}',
            ),
        ]);
        $client = self::createClient($requestHandler);

        $message = $client->peekLockMessage('queue-main', 10);

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame(
            'https://my-namespace.servicebus.windows.net/queue-main/messages/head?timeout=10',
            (string) $request->getUri(),
        );
        self::assertSame('secret', $request->getHeaderLine('X-Auth'));

        self::assertNotNull($message);
        self::assertSame('{"message":"test"}', $message->body);
        self::assertSame('application/json', $message->contentType);
        self::assertSame(self::LOCK_LOCATION, $message->lockLocation);
        self::assertSame('31907572164743c38741631acd554d6f', $message->brokerProperties->messageId);
        self::assertSame(2, $message->brokerProperties->deliveryCount);
        self::assertSame(2, $message->brokerProperties->sequenceNumber);
        self::assertSame('7da9cfd5-40d5-4bb1-8d64-ec5a52e1c547', $message->brokerProperties->lockToken);
    }

    public function testPeekLockMessageWithoutContentType(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                201,
                [
                    'BrokerProperties' => self::BROKER_PROPERTIES,
                    'Location' => self::LOCK_LOCATION,
                ],
                'body',
            ),
        ]);
        $client = self::createClient($requestHandler);

        $message = $client->peekLockMessage('queue-main', 10);

        self::assertNotNull($message);
        self::assertNull($message->contentType);
    }

    public function testPeekLockMessageWhenQueueIsEmpty(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(204)]);
        $client = self::createClient($requestHandler);

        self::assertNull($client->peekLockMessage('queue-main', 5));
        self::assertCount(1, $requestsHistory);
        self::assertSame(
            'https://my-namespace.servicebus.windows.net/queue-main/messages/head?timeout=5',
            (string) $requestsHistory[0]['request']->getUri(),
        );
    }

    /** @dataProvider provideInvalidPeekLockResponses */
    public function testPeekLockMessageWithInvalidResponse(array $headers, string $expectedError): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(201, $headers, 'body')]);
        $client = self::createClient($requestHandler);

        $this->expectException(InvalidResponseException::class);
        $this->expectExceptionMessage($expectedError);
        $this->expectExceptionCode(0);

        $client->peekLockMessage('queue-main', 10);
    }

    public static function provideInvalidPeekLockResponses(): Generator
    {
        yield 'missing broker properties' => [
            'headers' => ['Location' => self::LOCK_LOCATION],
            'expectedError' => 'Response BrokerProperties header is not valid JSON',
        ];

        yield 'incomplete broker properties' => [
            'headers' => ['BrokerProperties' => '{"MessageId":"id"}', 'Location' => self::LOCK_LOCATION],
            'expectedError' => 'Invalid response BrokerProperties: Expected the key "DeliveryCount" to exist.',
        ];

        yield 'missing lock location' => [
            'headers' => ['BrokerProperties' => self::BROKER_PROPERTIES],
            'expectedError' => 'Response is missing the Location header of the message lock',
        ];
    }

    /** @dataProvider provideInvalidWaitSeconds */
    public function testPeekLockMessageWithInvalidWait(int $waitSeconds, string $expectedError): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, []);
        $client = self::createClient($requestHandler, requestTimeout: 20);

        try {
            // @phpstan-ignore-next-line intentionally passing invalid values
            $client->peekLockMessage('queue-main', $waitSeconds);
            self::fail('Exception was expected');
        } catch (InvalidArgumentException $e) {
            self::assertSame($expectedError, $e->getMessage());
        }
        self::assertCount(0, $requestsHistory);
    }

    public static function provideInvalidWaitSeconds(): Generator
    {
        yield 'zero' => [
            'waitSeconds' => 0,
            'expectedError' => 'Expected a value greater than 0. Got: 0',
        ];

        yield 'equal to request timeout' => [
            'waitSeconds' => 20,
            'expectedError' =>
                'Wait time 20 must be shorter than the request timeout 20, the request would time out first',
        ];
    }

    public function testPeekLockMessageWaitJustBelowRequestTimeout(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(204)]);
        $client = self::createClient($requestHandler, requestTimeout: 20);

        self::assertNull($client->peekLockMessage('queue-main', 19));
        self::assertCount(1, $requestsHistory);
    }

    public function testDeleteMessage(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(200)]);
        $client = self::createClient($requestHandler);

        $client->deleteMessage(self::LOCK_LOCATION);

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('DELETE', $request->getMethod());
        self::assertSame(self::LOCK_LOCATION, (string) $request->getUri());
        self::assertSame('secret', $request->getHeaderLine('X-Auth'));
    }

    public function testDeleteMessageAcceptsExplicitHttpsPortAndHostCase(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(200)]);
        $client = new ServiceBusApiClient('https://My-Namespace.servicebus.windows.net', new ApiClientConfiguration(
            authenticator: new CustomHeaderAuth('X-Auth', 'secret'),
            requestHandler: $requestHandler(...),
        ));

        $client->deleteMessage('HTTPS://MY-NAMESPACE.servicebus.windows.net:443/queue-main/messages/2/lock');

        self::assertCount(1, $requestsHistory);
        self::assertSame(
            'https://my-namespace.servicebus.windows.net/queue-main/messages/2/lock',
            (string) $requestsHistory[0]['request']->getUri(),
        );
    }

    public function testDeleteMessageWithLostLock(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                404,
                ['Content-Type' => 'application/xml; charset=utf-8'],
                '<Error><Code>404</Code><Detail>The lock supplied is invalid.</Detail></Error>',
            ),
        ]);
        $client = self::createClient($requestHandler);

        try {
            $client->deleteMessage(self::LOCK_LOCATION);
            self::fail('Exception was expected');
        } catch (MessageLockLostException $e) {
            self::assertSame(404, $e->getCode());
            self::assertStringContainsString('The lock supplied is invalid.', $e->getMessage());
            self::assertInstanceOf(ClientException::class, $e->getPrevious());
        }
        self::assertCount(1, $requestsHistory);
    }

    public function testDeleteMessageWithOtherError(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(410, [], '<Error><Code>410</Code><Detail>Queue does not exist.</Detail></Error>'),
        ]);
        $client = self::createClient($requestHandler);

        try {
            $client->deleteMessage(self::LOCK_LOCATION);
            self::fail('Exception was expected');
        } catch (ClientException $e) {
            self::assertNotInstanceOf(MessageLockLostException::class, $e);
            self::assertSame(410, $e->getCode());
        }
    }

    /** @dataProvider provideForeignLockLocations */
    public function testDeleteMessageRejectsForeignLockLocation(string $lockLocation): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, []);
        $client = self::createClient($requestHandler);

        try {
            $client->deleteMessage($lockLocation);
            self::fail('Exception was expected');
        } catch (InvalidArgumentException $e) {
            self::assertSame(
                sprintf(
                    'Lock location "%s" does not belong to namespace "my-namespace.servicebus.windows.net"',
                    $lockLocation,
                ),
                $e->getMessage(),
            );
        }
        self::assertCount(0, $requestsHistory);
    }

    public static function provideForeignLockLocations(): Generator
    {
        yield 'other host' => ['https://attacker.example.com/queue-main/messages/2/lock'];
        yield 'host suffix' => ['https://my-namespace.servicebus.windows.net.example.com/queue-main/messages/2/lock'];
        yield 'plain http' => ['http://my-namespace.servicebus.windows.net/queue-main/messages/2/lock'];
        yield 'other port' => ['https://my-namespace.servicebus.windows.net:8443/queue-main/messages/2/lock'];
        yield 'user info' => ['https://user@my-namespace.servicebus.windows.net/queue-main/messages/2/lock'];
        yield 'relative' => ['/queue-main/messages/2/lock'];
        yield 'not an url' => ['http:///%'];
    }

    /** @dataProvider provideInvalidEndpoints */
    public function testInvalidEndpoint(string $endpoint): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Service Bus endpoint must be an https URL of the namespace, ' .
            'e.g. "https://<namespace>.servicebus.windows.net/"',
        );

        new ServiceBusApiClient($endpoint, new ApiClientConfiguration(
            authenticator: new CustomHeaderAuth('X-Auth', 'secret'),
        ));
    }

    public static function provideInvalidEndpoints(): Generator
    {
        yield 'sb scheme' => ['sb://my-namespace.servicebus.windows.net/'];
        yield 'http scheme' => ['http://my-namespace.servicebus.windows.net/'];
        yield 'no scheme' => ['my-namespace.servicebus.windows.net'];
        yield 'empty' => [''];
        yield 'not an url' => ['http:///%'];
    }

    /** @dataProvider provideInvalidEntityNames */
    public function testInvalidEntityName(string $queueName): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, []);
        $client = self::createClient($requestHandler);

        try {
            $client->sendMessage($queueName, new ServiceBusMessage('id', 'body', 'text/plain'));
            self::fail('Exception was expected');
        } catch (InvalidArgumentException $e) {
            self::assertStringStartsWith('Invalid Service Bus entity name', $e->getMessage());
        }
        self::assertCount(0, $requestsHistory);
    }

    public static function provideInvalidEntityNames(): Generator
    {
        yield 'empty' => [''];
        yield 'path traversal' => ['../other'];
        yield 'query injection' => ['queue?timeout=1'];
        yield 'absolute url' => ['https://attacker.example.com/queue'];
        yield 'leading slash' => ['/queue'];
        yield 'trailing slash' => ['queue/'];
        yield 'trailing dot' => ['queue.'];
        yield 'empty segment' => ['topic//sub'];
        yield 'encoded' => ['queue%2Fother'];
        yield 'trailing newline' => ["queue\n"];
    }

    public function testRedirectIsNotFollowed(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(307, ['Location' => 'https://attacker.example.com/queue-main/messages']),
            new Response(201),
        ]);
        $client = self::createClient($requestHandler);

        try {
            $client->sendMessage('queue-main', new ServiceBusMessage('id', 'body', 'text/plain'));
            self::fail('Exception was expected');
        } catch (ClientException $e) {
            self::assertSame(307, $e->getCode());
        }
        self::assertCount(1, $requestsHistory);
        self::assertSame('my-namespace.servicebus.windows.net', $requestsHistory[0]['request']->getUri()->getHost());
    }

    public function testValidEntityNames(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(201), new Response(201)]);
        $client = self::createClient($requestHandler);

        $client->sendMessage('q', new ServiceBusMessage('id', 'body', 'text/plain'));
        $client->sendMessage('My.Queue-1_', new ServiceBusMessage('id', 'body', 'text/plain'));

        self::assertCount(2, $requestsHistory);
        self::assertSame('/q/messages', $requestsHistory[0]['request']->getUri()->getPath());
        self::assertSame('/My.Queue-1_/messages', $requestsHistory[1]['request']->getUri()->getPath());
    }

    private static function createClient(HandlerStack $requestHandler, int $requestTimeout = 120): ServiceBusApiClient
    {
        return new ServiceBusApiClient(self::ENDPOINT, new ApiClientConfiguration(
            backoffMaxTries: 0,
            authenticator: new CustomHeaderAuth('X-Auth', 'secret'),
            requestHandler: $requestHandler(...),
            requestTimeout: $requestTimeout, // @phpstan-ignore-line
        ));
    }

    /**
     * @param list<array{request: Request, response: Response}> $requestsHistory
     * @param list<Response> $responses
     */
    private static function createRequestHandler(?array &$requestsHistory, array $responses): HandlerStack
    {
        $requestsHistory = [];

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));

        return $stack;
    }
}
