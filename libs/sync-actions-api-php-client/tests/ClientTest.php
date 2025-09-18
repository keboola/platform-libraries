<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\SyncActionsClient\ActionData;
use Keboola\SyncActionsClient\Client;
use Keboola\SyncActionsClient\Exception\ClientException;
use Keboola\SyncActionsClient\Exception\ResponseException;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class ClientTest extends TestCase
{
    private function getClient(array $options): Client
    {
        return new Client(
            'http://example.com/',
            'testToken',
            $options,
        );
    }

    public function testCreateClientInvalidBackoff(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "abc" is invalid: This value should be a valid number',
        );
        new Client(
            'http://example.com/',
            'testToken',
            ['backoffMaxTries' => 'abc'],
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
            ['backoffMaxTries' => -1],
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
            ['backoffMaxTries' => 101],
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
        $client = $this->getClient(['handler' => $stack]);
        $job = $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
        self::assertEquals('bar', $job['foo']);
        self::assertCount(1, $requestHistory);
        /** @var Request $request */
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
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job data: Type is not supported');
        $res = fopen(sys_get_temp_dir() . '/touch', 'w');
        $client->callAction(new ActionData('keboola.ex-db-storage', '123', ['foo' => $res]));
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

        $client = $this->getClient(['handler' => $requestHandler, 'backoffMaxTries' => 0]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Error on server');
        $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
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

        $client = $this->getClient(['handler' => $requestHandler]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Unable to parse response body into JSON: ');
        $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
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

        $client = $this->getClient(['handler' => $requestHandler]);

        $this->expectException(ResponseException::class);
        $this->expectExceptionMessage('400 Bad Request');
        $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
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

        $client = $this->getClient(['handler' => $requestHandler]);

        try {
            $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
        } catch (ResponseException $e) {
            self::assertTrue($e->isErrorCode('some.error'));
        }
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
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $logger = new TestLogger();
        $client = $this->getClient(['handler' => $stack, 'logger' => $logger, 'userAgent' => 'test agent']);
        $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('test agent', $request->getHeader('User-Agent')[0]);
        self::assertTrue($logger->hasInfoThatContains('"POST  /1.1" 200 '));
        self::assertTrue($logger->hasInfoThatContains('test agent'));
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
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        $job = $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
        self::assertEquals('bar', $job['foo']);
        self::assertCount(3, $requestHistory);
        /** @var Request $request */
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
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack, 'backoffMaxTries' => 1]);
        try {
            $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(2, $requestHistory);
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
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack, 'backoffMaxTries' => 3]);
        try {
            $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(4, $requestHistory);
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
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('{"message": "Unauthorized"}');
        $client->callAction(new ActionData('keboola.ex-db-storage', '123'));
    }

    public function testGetActions(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '["action1", "action2"]',
            ),
        ]);

        $client = $this->getClient(['handler' => $mock]);
        $actions = $client->getActions('keboola.ex-db-storage');

        self::assertEquals(['action1', 'action2'], $actions);
    }
}
