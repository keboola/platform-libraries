<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\SyncActionsClient\ActionData;
use Keboola\SyncActionsClient\ApiClientConfiguration;
use Keboola\SyncActionsClient\Client;
use Keboola\SyncActionsClient\Exception\ClientException;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
    private function getClient(ApiClientConfiguration $options): Client
    {
        return new Client(
            'http://example.com/',
            'testToken',
            $options,
        );
    }

    public function testCreateClientTooLowBackoff(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "-1" is invalid: This value should be between 0 and 100.',
        );
        new Client(
            'http://example.com/',
            'testToken',
            // @phpstan-ignore-next-line
            new ApiClientConfiguration(backoffMaxTries: -1),
        );
    }

    public function testCreateClientTooHighBackoff(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "101" is invalid: This value should be between 0 and 100.',
        );
        new Client(
            'http://example.com/',
            'testToken',
            new ApiClientConfiguration(backoffMaxTries: 101),
        );
    }

    public function testCreateClientInvalidToken(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "" is invalid: This value should not be blank.',
        );
        new Client('http://example.com/', '');
    }

    public function testCreateClientInvalidUrl(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "invalid url" is invalid: This value is not a valid URL.',
        );
        new Client('invalid url', 'testToken');
    }

    public function testCreateClientMultipleErrors(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "invalid url" is invalid: This value is not a valid URL.'
            . "\n" . 'Value "" is invalid: This value should not be blank.' . "\n",
        );
        new Client('invalid url', '');
    }

    public function testClientRequestResponse(): void
    {
        $mock = new MockHandler([
            new Response(
                201,
                ['Content-Type' => 'application/json'],
                '{
                    "foo": "bar"
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(new ApiClientConfiguration(requestHandler: $stack(...)));

        $job = $client->callAction(new ActionData('keboola.runner-config-test', '123'));

        self::assertEquals('bar', $job->data->foo);
        /** @var array<array{request: Request}> $requestHistory */
        self::assertCount(1, $requestHistory);
        $request = $requestHistory[0]['request'];
        self::assertEquals('http://example.com/actions', $request->getUri()->__toString());
        self::assertEquals('POST', $request->getMethod());
        self::assertEquals('testToken', $request->getHeader('X-StorageApi-Token')[0]);
        self::assertEquals('Sync Actions PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testInvalidResponse(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                'invalid json',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $requestHandler = HandlerStack::create($mock);
        $requestHandler->push($history);

        $client = $this->getClient(new ApiClientConfiguration(requestHandler: $requestHandler(...)));

        $res = fopen(sys_get_temp_dir() . '/touch', 'w');
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job data: Type is not supported');
        $client->callAction(new ActionData('keboola.runner-config-test', '123', ['foo' => $res]));
    }

    public function testClientExceptionIsThrownWhenGuzzleRequestErrorOccurs(): void
    {
        $requestHandler = MockHandler::createWithMiddleware([
            new Response(
                500,
                ['Content-Type' => 'text/plain'],
                'Error on server',
            ),
        ]);

        $client = $this->getClient(new ApiClientConfiguration(
            backoffMaxTries: 0,
            requestHandler: $requestHandler(...),
        ));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Error on server');
        $client->callAction(new ActionData('keboola.runner-config-test', '123'));
    }

    public function testClientExceptionIsThrownForResponseWithInvalidJson(): void
    {
        $requestHandler = MockHandler::createWithMiddleware([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{not a valid json]',
            ),
        ]);

        $client = $this->getClient(new ApiClientConfiguration(requestHandler: $requestHandler(...)));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Response is not a valid JSON: Syntax error');
        $client->callAction(new ActionData('keboola.runner-config-test', '123'));
    }

    public function testRequestExceptionIsThrownForValidErrorResponse(): void
    {
        $requestHandler = MockHandler::createWithMiddleware([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                (string) json_encode([]),
            ),
        ]);

        $client = $this->getClient(new ApiClientConfiguration(requestHandler: $requestHandler(...)));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Response is not a valid error response: []');
        $client->callAction(new ActionData('keboola.runner-config-test', '123'));
    }

    public function testRequestExceptionIsThrownForErrorResponseWithErrorCode(): void
    {
        $requestHandler = MockHandler::createWithMiddleware([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                (string) json_encode([
                    'context' => [
                        'errorCode' => 'some.error',
                    ],
                ]),
            ),
        ]);

        $client = $this->getClient(new ApiClientConfiguration(requestHandler: $requestHandler(...)));

        $this->expectExceptionMessageMatches(
            '#Client error: `POST http:\/\/example\.com\/actions` resulted in a `400 Bad Request`'
            . ' response:.*{"context":{"errorCode":"some\.error"}}#s',
        );
        $this->expectException(ClientException::class);
        $client->callAction(new ActionData('keboola.runner-config-test', '123'));
    }

    public function testLogger(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "foo": "bar"
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $requestHandler = HandlerStack::create($mock);
        $requestHandler->push($history);
        $logHandler = new TestHandler();
        $logger = new Logger(name: 'test', handlers: [$logHandler]);

        $client = $this->getClient(new ApiClientConfiguration(
            userAgent: 'test agent',
            requestHandler: $requestHandler(...),
            logger: $logger,
        ));

        $client->callAction(new ActionData('keboola.runner-config-test', '123'));

        /** @var array<array{request: Request}> $requestHistory */
        $request = $requestHistory[0]['request'];
        self::assertEquals('Sync Actions PHP Client - test agent', $request->getHeader('User-Agent')[0]);
        self::assertTrue($logHandler->hasInfoThatContains('"POST  /1.1" 200 '));
        self::assertTrue($logHandler->hasInfoThatContains('test agent'));
    }

    public function testRetrySuccess(): void
    {
        $mock = new MockHandler([
            new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}',
            ),
            new Response(
                501,
                ['Content-Type' => 'application/json'],
                'Out of order',
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "foo": "bar"
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $requestHandler = HandlerStack::create($mock);
        $requestHandler->push($history);
        $client = $this->getClient(new ApiClientConfiguration(requestHandler: $requestHandler(...)));

        $job = $client->callAction(new ActionData('keboola.runner-config-test', '123'));

        self::assertEquals('bar', $job->data->foo);
        /** @var array<array{request: Request}> $requestHistory */
        self::assertCount(3, $requestHistory);
        $request = $requestHistory[0]['request'];
        self::assertEquals('http://example.com/actions', $request->getUri()->__toString());
        $request = $requestHistory[1]['request'];
        self::assertEquals('http://example.com/actions', $request->getUri()->__toString());
        $request = $requestHistory[2]['request'];
        self::assertEquals('http://example.com/actions', $request->getUri()->__toString());
    }

    public function testRetryFailure(): void
    {
        $responses = [];
        for ($i = 0; $i < 30; $i++) {
            $responses[] = new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}',
            );
        }
        $mock = new MockHandler($responses);
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $requestStack = HandlerStack::create($mock);
        $requestStack->push($history);
        $client = $this->getClient(new ApiClientConfiguration(
            backoffMaxTries: 1,
            requestHandler: $requestStack(...),
        ));

        try {
            $client->callAction(new ActionData('keboola.runner-config-test', '123'));
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(2, (array) $requestHistory);
    }

    public function testRetryFailureReducedBackoff(): void
    {
        $responses = [];
        for ($i = 0; $i < 30; $i++) {
            $responses[] = new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}',
            );
        }
        $mock = new MockHandler($responses);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $requestStack = HandlerStack::create($mock);
        $requestStack->push($history);
        $client = $this->getClient(new ApiClientConfiguration(
            backoffMaxTries: 3,
            requestHandler: $requestStack(...),
        ));

        try {
            $client->callAction(new ActionData('keboola.runner-config-test', '123'));
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(4, (array) $requestHistory);
    }

    public function testNoRetry(): void
    {
        $mock = new MockHandler([
            new Response(
                401,
                ['Content-Type' => 'application/json'],
                '{"message": "Unauthorized"}',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $requestStack = HandlerStack::create($mock);
        $requestStack->push($history);
        $client = $this->getClient(new ApiClientConfiguration(
            requestHandler: $requestStack(...),
        ));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('{"message": "Unauthorized"}');
        $client->callAction(new ActionData('keboola.runner-config-test', '123'));
    }

    public function testGetActions(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{"actions": ["action1", "action2"]}',
            ),
        ]);
        $requestStack = HandlerStack::create($mock);

        $client = $this->getClient(new ApiClientConfiguration(
            backoffMaxTries: 3,
            requestHandler: $requestStack(...),
        ));
        $actions = $client->getActions('keboola.runner-config-test');

        self::assertEquals(['action1', 'action2'], $actions->actions);
    }


    public function testGetActionsInvalidResponse(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{"broken": ["action1", "action2"]}',
            ),
        ]);
        $requestStack = HandlerStack::create($mock);

        $client = $this->getClient(new ApiClientConfiguration(
            backoffMaxTries: 3,
            requestHandler: $requestStack(...),
        ));

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Failed to parse response');
        $client->getActions('keboola.runner-config-test');
    }
}
