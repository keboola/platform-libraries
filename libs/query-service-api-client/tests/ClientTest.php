<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests;

use Generator;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\QueryApi\Client;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
    public function testConstructorRequiresUrl(): void
    {
        self::expectException(InvalidArgumentException::class);
        self::expectExceptionMessage('url must be set');

        new Client([
            'token' => 'test-token',
        ]);
    }

    public function testConstructorRequiresToken(): void
    {
        self::expectException(InvalidArgumentException::class);
        self::expectExceptionMessage('token must be set');

        new Client([
            'url' => 'https://test.keboola.com',
        ]);
    }

    public function testSubmitQueryJob(): void
    {
        $mockHandler = new MockHandler([
            new Response(201, [], json_encode(['queryJobId' => 'job-12345']) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->submitQueryJob('main', 'workspace-123', [
            'statements' => ['SELECT * FROM table1'],
            'transactional' => true,
        ]);

        self::assertEquals('job-12345', $result->getQueryJobId());
    }

    public function testGetJobStatus(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'queryJobId' => 'job-12345',
                'status' => 'running',
                'statements' => [],
            ]) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->getJobStatus('job-12345');

        self::assertEquals('job-12345', $result->getQueryJobId());
        self::assertEquals('running', $result->getStatus());
    }

    public function testCancelJob(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'queryJobId' => 'job-12345',
                'status' => 'canceled',
                'statements' => [],
            ]) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->cancelJob('job-12345', ['reason' => 'User requested']);

        self::assertEquals('job-12345', $result->getQueryJobId());
    }

    public function testGetJobResults(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'data' => [['id' => 1, 'name' => 'test']],
                'status' => 'completed',
                'rowsAffected' => 1,
                'columns' => [],
            ]) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->getJobResults('job-12345', 'stmt-67890');

        self::assertEquals('completed', $result->getStatus());
        self::assertEquals(1, $result->getRowsAffected());
        self::assertCount(1, $result->getData());
    }

    public function testHealthCheck(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'service' => 'query',
                'status' => 'ok',
                'timestamp' => '2024-01-01T00:00:00Z',
                'version' => '1.0.0',
            ]) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->healthCheck();

        self::assertEquals('ok', $result->getStatus());
        self::assertTrue($result->isOk());
    }

    public function testHealthCheckWithInvalidToken(): void
    {
        // Health check should work even with invalid token since no auth is required
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'service' => 'query',
                'status' => 'ok',
                'timestamp' => '2024-01-01T00:00:00Z',
                'version' => '1.0.0',
            ]) ?: ''),
        ]);

        // Create client with completely invalid token
        $handlerStack = HandlerStack::create($mockHandler);
        $client = new Client([
            'url' => 'https://query.test.keboola.com',
            'token' => 'completely-invalid-token-that-would-fail-auth',
            'handler' => $handlerStack,
        ]);

        // Health check should succeed because no token is sent
        $result = $client->healthCheck();

        self::assertEquals('ok', $result->getStatus());
        self::assertTrue($result->isOk());
    }

    /**
     * @param array{
     *     url: string,
     *     token: string,
     *     backoffMaxTries?: int,
     *     userAgent?: string,
     *     runId?: string,
     *     handler?: HandlerStack,
     * } $clientConfig
     * @param array<string, array<string>> $expectedHeaders
     * @dataProvider requestHeadersDataProvider
     */
    public function testRequestHeaders(
        array $clientConfig,
        string $method,
        ?string $jobId,
        array $expectedHeaders,
    ): void {
        $requestHeaders = [];
        $mockHandler = new MockHandler([new Response(200, [], '{}')]);

        // Create handler stack without custom middleware first
        $handlerStack = HandlerStack::create($mockHandler);

        $config = array_merge([
            'url' => 'https://query.test.keboola.com',
            'handler' => $handlerStack,
        ], $clientConfig);

        $client = new Client($config);

        // Add middleware after client is created to capture headers after Client's middleware runs
        $handlerStack->push(function (callable $handler) use (&$requestHeaders) {
            return function ($request, array $options) use ($handler, &$requestHeaders) {
                $requestHeaders = $request->getHeaders();
                return $handler($request, $options);
            };
        });

        match ($method) {
            'healthCheck' => $client->healthCheck(),
            'getJobStatus' => $client->getJobStatus($jobId),
            default => throw new InvalidArgumentException("Unknown method: $method")
        };

        self::assertSame(
            $expectedHeaders,
            $requestHeaders,
        );
    }

    public static function requestHeadersDataProvider(): Generator
    {
        yield 'health-check includes base headers only' => [
            'clientConfig' => ['token' => 'test-token'],
            'method' => 'healthCheck',
            'jobId' => null,
            'expectedHeaders' => [
                'Host' => ['query.test.keboola.com'],
                'User-Agent' => ['Keboola Query API PHP Client'],
                'Content-Type' => ['application/json'],
            ],
        ];

        yield 'authenticated endpoint includes auth token' => [
            'clientConfig' => ['token' => 'auth-token-123'],
            'method' => 'getJobStatus',
            'jobId' => 'job-123',
            'expectedHeaders' => [
                'Host' => ['query.test.keboola.com'],
                'User-Agent' => ['Keboola Query API PHP Client'],
                'Content-Type' => ['application/json'],
                'X-StorageAPI-Token' => ['auth-token-123'],
            ],
        ];

        yield 'runId header included when configured' => [
            'clientConfig' => ['token' => 'test-token', 'runId' => 'run-456'],
            'method' => 'getJobStatus',
            'jobId' => 'job-123',
            'expectedHeaders' => [
                'Host' => ['query.test.keboola.com'],
                'User-Agent' => ['Keboola Query API PHP Client'],
                'Content-Type' => ['application/json'],
                'X-KBC-RunId' => ['run-456'],
                'X-StorageAPI-Token' => ['test-token'],
            ],
        ];

        yield 'custom userAgent properly appended' => [
            'clientConfig' => ['token' => 'test-token', 'userAgent' => 'MyApp/2.0'],
            'method' => 'healthCheck',
            'jobId' => null,
            'expectedHeaders' => [
                'Host' => ['query.test.keboola.com'],
                'User-Agent' => ['Keboola Query API PHP Client MyApp/2.0'],
                'Content-Type' => ['application/json'],
            ],
        ];
    }

    private function createClientWithMockHandler(MockHandler $mockHandler): Client
    {
        $handlerStack = HandlerStack::create($mockHandler);

        return new Client([
            'url' => 'https://query.test.keboola.com',
            'token' => 'test-token',
            'handler' => $handlerStack,
        ]);
    }
}
