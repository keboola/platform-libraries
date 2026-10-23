<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\EventGrid;

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
use Keboola\AzureApiClient\EventGrid\EventGridApiClient;
use Keboola\AzureApiClient\EventGrid\Model\EventGridEvent;
use Keboola\AzureApiClient\EventGrid\Resources;
use Keboola\AzureApiClient\Exception\ClientException;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use stdClass;

class EventGridApiClientTest extends TestCase
{
    private const TOPIC_HOSTNAME = 'my-topic.westeurope-1.eventgrid.azure.net';

    public function testPublishEvents(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(200)]);
        $client = new EventGridApiClient(self::TOPIC_HOSTNAME, new ApiClientConfiguration(
            authenticator: new CustomHeaderAuth('aeg-sas-key', 'access-key'),
            requestHandler: $requestHandler(...),
        ));

        $client->publishEvents([
            new EventGridEvent(
                'event-1',
                'subject-1',
                ['foo' => 'bar'],
                'Keboola.Test',
                new DateTimeImmutable('2024-03-05T12:15:16Z'),
            ),
            new EventGridEvent(
                'event-2',
                'subject-2',
                [],
                'Keboola.Test',
                new DateTimeImmutable('2024-03-05T12:15:17Z'),
            ),
        ]);

        self::assertCount(1, $requestsHistory);
        $request = $requestsHistory[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame(
            'https://my-topic.westeurope-1.eventgrid.azure.net/api/events?api-version=2018-01-01',
            (string) $request->getUri(),
        );
        self::assertSame('access-key', $request->getHeaderLine('aeg-sas-key'));
        self::assertSame('application/json', $request->getHeaderLine('Content-Type'));
        self::assertSame(
            '[' .
            '{"id":"event-1","subject":"subject-1","data":{"foo":"bar"},"eventType":"Keboola.Test",' .
            '"eventTime":"2024-03-05T12:15:16Z","dataVersion":"1.0"},' .
            '{"id":"event-2","subject":"subject-2","data":[],"eventType":"Keboola.Test",' .
            '"eventTime":"2024-03-05T12:15:17Z","dataVersion":"1.0"}' .
            ']',
            (string) $request->getBody(),
        );
    }

    public function testAuthenticatesForEventGridResource(): void
    {
        $authenticatorFactory = $this->createMock(RequestAuthenticatorFactoryInterface::class);
        $authenticatorFactory->expects(self::once())
            ->method('createRequestAuthenticator')
            ->with(Resources::AZURE_EVENT_GRID)
            ->willReturn(new class implements RequestAuthenticatorInterface {
                public function __invoke(RequestInterface $request): RequestInterface
                {
                    return $request->withHeader('Authorization', 'Bearer entra-token');
                }
            });

        $requestHandler = self::createRequestHandler($requestsHistory, [new Response(200)]);
        $client = new EventGridApiClient(self::TOPIC_HOSTNAME, new ApiClientConfiguration(
            authenticator: $authenticatorFactory,
            requestHandler: $requestHandler(...),
        ));
        $client->publishEvents([new EventGridEvent('id', 'subject', [], 'Keboola.Test')]);

        self::assertCount(1, $requestsHistory);
        self::assertSame('Bearer entra-token', $requestsHistory[0]['request']->getHeaderLine('Authorization'));
    }

    public function testPublishEventsFailure(): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, [
            new Response(
                401,
                ['Content-Type' => 'application/json'],
                '{"error":{"code":"Unauthorized","message":"The request authorization key is not authorized."}}',
            ),
        ]);
        $client = new EventGridApiClient(self::TOPIC_HOSTNAME, new ApiClientConfiguration(
            authenticator: new CustomHeaderAuth('aeg-sas-key', 'wrong-key'),
            requestHandler: $requestHandler(...),
        ));

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(401);
        $this->expectExceptionMessage('Unauthorized: The request authorization key is not authorized.');

        $client->publishEvents([new EventGridEvent('id', 'subject', [], 'Keboola.Test')]);
    }

    /** @dataProvider provideInvalidEvents */
    public function testPublishInvalidEvents(array $events): void
    {
        $requestHandler = self::createRequestHandler($requestsHistory, []);
        $client = new EventGridApiClient(self::TOPIC_HOSTNAME, new ApiClientConfiguration(
            authenticator: new CustomHeaderAuth('aeg-sas-key', 'access-key'),
            requestHandler: $requestHandler(...),
        ));

        try {
            // @phpstan-ignore-next-line intentionally passing invalid values
            $client->publishEvents($events);
            self::fail('Exception was expected');
        } catch (InvalidArgumentException) {
        }
        self::assertCount(0, $requestsHistory);
    }

    public static function provideInvalidEvents(): Generator
    {
        yield 'empty' => [[]];
        yield 'not an event' => [[new stdClass()]];
    }

    /** @dataProvider provideInvalidHostnames */
    public function testInvalidTopicHostname(string $hostname): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Event Grid topic hostname must be a bare hostname, e.g. "<topic>.<region>-1.eventgrid.azure.net"',
        );

        new EventGridApiClient($hostname, new ApiClientConfiguration(
            authenticator: new CustomHeaderAuth('aeg-sas-key', 'access-key'),
        ));
    }

    public static function provideInvalidHostnames(): Generator
    {
        yield 'empty' => [''];
        yield 'url' => ['https://my-topic.westeurope-1.eventgrid.azure.net/api/events'];
        yield 'with path' => ['my-topic.westeurope-1.eventgrid.azure.net/api/events'];
        yield 'with port' => ['my-topic.westeurope-1.eventgrid.azure.net:443'];
        yield 'single label' => ['localhost'];
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
