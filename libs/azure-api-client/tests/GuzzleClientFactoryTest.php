<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use GuzzleHttp\Exception\ClientException as GuzzleClientException;
use GuzzleHttp\Exception\ServerException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\GuzzleClientFactory;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class GuzzleClientFactoryTest extends TestCase
{
    public function testGetClient(): void
    {
        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('http://example.com');

        self::assertInstanceOf(NullLogger::class, $factory->getLogger());
        self::assertEquals(120, $client->getConfig('timeout'));
        self::assertEquals(10, $client->getConfig('connect_timeout'));
    }

    /**
     * @dataProvider invalidOptionsProvider
     */
    public function testInvalidOptions(array $options, string $expectedMessage): void
    {
        $factory = new GuzzleClientFactory(new NullLogger());
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectedMessage);
        $factory->getClient('http://example.com', $options);
    }

    public function invalidOptionsProvider(): array
    {
        return [
            'invalid-options' => [
                [
                    'non-existent' => 'foo',
                ],
                // phpcs:ignore Generic.Files.LineLength
                'Invalid options when creating client: non-existent. Valid options are: backoffMaxTries, userAgent, handler, logger.',
            ],
            'invalid-backoff' => [
                [
                    'backoffMaxTries' => 'foo',
                ],
                'Invalid options when creating client: Value "foo" is invalid: This value should be a valid number.',
            ],
        ];
    }

    public function testInvalidUrl(): void
    {
        $factory = new GuzzleClientFactory(new NullLogger());
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('boo');
        $factory->getClient('boo');
    }

    public function testLogger(): void
    {
        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient('https://example.com', ['logger' => $logger, 'userAgent' => 'test-client']);
        $client->get('');
        self::assertTrue($logsHandler->hasInfoThatContains('test-client - ['));
        self::assertTrue($logsHandler->hasInfoThatContains('"GET  /1.1" 200'));
    }

    public function testDefaultHeader(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                [],
                'boo'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient(
            'https://example.com',
            ['handler' => $stack, 'userAgent' => 'test-client']
        );
        $client->get('');

        self::assertCount(1, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        self::assertEquals('GET', $request->getMethod());
        self::assertEquals('test-client', $request->getHeader('User-Agent')[0]);
        // default header
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testRetryDeciderNoRetry(): void
    {
        $mock = new MockHandler([
            new Response(
                403,
                [],
                'boo'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient(
            'https://example.com',
            ['handler' => $stack, 'userAgent' => 'test-client']
        );
        try {
            $client->get('');
            self::fail('Must throw exception');
        } catch (GuzzleClientException $e) {
            self::assertStringContainsString(
                'Client error: `GET https://example.com` resulted in a `403 Forbidden` response',
                $e->getMessage()
            );
        }

        self::assertCount(1, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
    }

    public function testRetryDeciderRetryFail(): void
    {
        $mock = new MockHandler([
            new Response(
                501,
                [],
                'boo'
            ),
            new Response(
                501,
                [],
                'boo'
            ),
            new Response(
                501,
                [],
                'boo'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $factory = new GuzzleClientFactory(new NullLogger());
        $client = $factory->getClient(
            'https://example.com',
            ['handler' => $stack, 'userAgent' => 'test-client', 'backoffMaxTries' => 2]
        );
        try {
            $client->get('');
            self::fail('Must throw exception');
        } catch (ServerException $e) {
            self::assertStringContainsString(
                'Server error: `GET https://example.com` resulted in a `501 Not Implemented`',
                $e->getMessage()
            );
        }

        self::assertCount(3, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        $request = $requestHistory[1]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        $request = $requestHistory[2]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
    }

    public function testRetryDeciderRetrySuccess(): void
    {
        $mock = new MockHandler([
            new Response(
                501,
                [],
                'boo'
            ),
            new Response(
                501,
                [],
                'boo'
            ),
            new Response(
                200,
                [],
                'boo'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $factory = new GuzzleClientFactory($logger);
        $client = $factory->getClient(
            'https://example.com',
            ['handler' => $stack, 'userAgent' => 'test-client', 'backoffMaxTries' => 2]
        );
        $client->get('');

        self::assertCount(3, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        $request = $requestHistory[1]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        $request = $requestHistory[2]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        self::assertTrue($logsHandler->hasWarningThatContains(
            'Request failed (Server error: `GET https://example.com` resulted in a `501 Not Implemented`'
        ));
        self::assertTrue($logsHandler->hasWarningThatContains('retrying (1 of 2)'));
    }

    public function testRetryDeciderThrottlingRetrySuccess(): void
    {
        $mock = new MockHandler([
            new Response(
                429,
                [],
                'boo'
            ),
            new Response(
                429,
                [],
                'boo'
            ),
            new Response(
                200,
                [],
                'boo'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $factory = new GuzzleClientFactory($logger);
        $client = $factory->getClient(
            'https://example.com',
            ['handler' => $stack, 'userAgent' => 'test-client', 'backoffMaxTries' => 2]
        );
        $client->get('');

        self::assertCount(3, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        $request = $requestHistory[1]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        $request = $requestHistory[2]['request'];
        self::assertEquals('https://example.com', $request->getUri()->__toString());
        self::assertTrue($logsHandler->hasWarningThatContains(
            'Request failed (Client error: `GET https://example.com` resulted in a `429 Too Many Requests`'
        ));
        self::assertTrue($logsHandler->hasWarningThatContains('retrying (1 of 2)'));
    }
}
