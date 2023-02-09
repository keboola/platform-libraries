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

        self::assertEquals(120, $client->getConfig('timeout'));
        self::assertEquals(10, $client->getConfig('connect_timeout'));
    }

    /** @dataProvider provideInvalidUrls */
    public function testInvalidUrl(string $url, string $expectedError): void
    {
        $factory = new GuzzleClientFactory(new NullLogger());

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectedError);

        $factory->getClient($url);
    }

    public function provideInvalidUrls(): iterable
    {
        yield 'empty string' => [
            'url' => '',
            'error' => 'Invalid options when creating client: Value "" is invalid: This value should not be blank.',
        ];

        yield 'invalid URL' => [
            'url' => 'foo',
            'error' => 'Invalid options when creating client: Value "foo" is invalid: This value is not a valid URL.',
        ];
    }

    /** @dataProvider provideInvalidOptions */
    public function testInvalidOptions(array $options, string $expectedMessage): void
    {
        $factory = new GuzzleClientFactory(new NullLogger());
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectedMessage);
        $factory->getClient('http://example.com', $options);
    }

    public function provideInvalidOptions(): iterable
    {
        yield 'invalid options' => [
            'options' => [
                'non-existent' => 'foo',
            ],
            // phpcs:ignore Generic.Files.LineLength
            'error' => 'Invalid options when creating client: non-existent. Valid options are: backoffMaxTries, userAgent, middleware.',
        ];

        yield 'invalid backoff' => [
            [
                'backoffMaxTries' => 'foo',
            ],
            'Invalid options when creating client: Value "foo" is invalid: This value should be a valid number.',
        ];

        yield 'high backoff' => [
            [
                'backoffMaxTries' => 101,
            ],
            'Invalid options when creating client: Value "101" is invalid: This value should be between 0 and 100.',
        ];

        yield 'low backoff' => [
            [
                'backoffMaxTries' => -1,
            ],
            'Invalid options when creating client: Value "-1" is invalid: This value should be between 0 and 100.',
        ];
    }

    public function testLogger(): void
    {
        $logsHandler = new TestHandler();
        $logger = new Logger('tests', [$logsHandler]);

        $factory = new GuzzleClientFactory($logger);
        $client = $factory->getClient('https://example.com', ['userAgent' => 'test-client']);
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

        $factory = new GuzzleClientFactory(new NullLogger(), $stack);
        $client = $factory->getClient(
            'https://example.com',
            ['userAgent' => 'test-client']
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

        $factory = new GuzzleClientFactory(new NullLogger(), $stack);
        $client = $factory->getClient(
            'https://example.com',
            ['userAgent' => 'test-client']
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

        $factory = new GuzzleClientFactory(new NullLogger(), $stack);
        $client = $factory->getClient(
            'https://example.com',
            ['userAgent' => 'test-client', 'backoffMaxTries' => 2]
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

        $factory = new GuzzleClientFactory($logger, $stack);
        $client = $factory->getClient(
            'https://example.com',
            ['userAgent' => 'test-client', 'backoffMaxTries' => 2]
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

        $factory = new GuzzleClientFactory($logger, $stack);
        $client = $factory->getClient(
            'https://example.com',
            ['userAgent' => 'test-client', 'backoffMaxTries' => 2]
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
