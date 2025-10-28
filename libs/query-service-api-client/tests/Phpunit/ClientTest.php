<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit;

use Generator;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use Keboola\QueryApi\Client;
use Keboola\QueryApi\ClientException;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
    public function testConstructorRequiresUrl(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid parameters');

        new Client([
            'token' => 'test-token',
        ]);
    }

    public function testConstructorRequiresToken(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid parameters');

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

        $response = $client->submitQueryJob('main', 'workspace-123', [
            'statements' => ['SELECT * FROM table1'],
            'transactional' => true,
        ]);

        self::assertEquals('job-12345', $response->getQueryJobId());
    }

    public function testGetJobStatus(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'queryJobId' => 'job-12345',
                'status' => 'running',
                'actorType' => 'user',
                'createdAt' => '2024-01-01T00:00:00Z',
                'changedAt' => '2024-01-01T00:00:00Z',
                'statements' => [],
            ]) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $response = $client->getJobStatus('job-12345');

        self::assertEquals('job-12345', $response->getQueryJobId());
        self::assertEquals('running', $response->getStatus());
        self::assertEquals([], $response->getStatements());
    }

    public function testCancelJob(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode(['queryJobId' => 'job-12345']) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $response = $client->cancelJob('job-12345', ['reason' => 'User requested']);

        self::assertEquals('job-12345', $response->getQueryJobId());
    }

    public function testGetJobResults(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'data' => [['id' => 1, 'name' => 'test']],
                'status' => 'completed',
                'numberOfRows' => 1,
                'rowsAffected' => 1,
            ]) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $response = $client->getJobResults('job-12345', 'stmt-67890');

        self::assertEquals('completed', $response->getStatus());
        self::assertEquals(1, $response->getRowsAffected());
        // @phpstan-ignore-next-line
        self::assertIsArray($response->getColumns());
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
        $mockHandler = new MockHandler([new Response(200, [], json_encode([
            'queryJobId' => 'job-123',
            'status' => 'running',
            'actorType' => 'user',
            'createdAt' => '2024-01-01T00:00:00Z',
            'changedAt' => '2024-01-01T00:00:00Z',
            'statements' => [],
        ]) ?: '')]);

        // Create handler stack without custom middleware first
        $handlerStack = HandlerStack::create($mockHandler);

        $config = array_merge([
            'url' => 'https://query.test.keboola.com',
            'handler' => $handlerStack,
        ], $clientConfig);

        $client = new Client($config);

        // Add middleware after client is created to capture headers after Client's middleware runs
        /** @var array<string, array<string>> $requestHeaders */
        $handlerStack->push(function (callable $handler) use (&$requestHeaders) {
            return function ($request, array $options) use ($handler, &$requestHeaders) {
                /** @var \Psr\Http\Message\RequestInterface $request */
                $requestHeaders = $request->getHeaders();
                return $handler($request, $options);
            };
        });

        match ($method) {
            'getJobStatus' => $client->getJobStatus($jobId ?? ''),
            default => throw new InvalidArgumentException("Unknown method: $method")
        };

        // Check each expected header exists with correct value
        foreach ($expectedHeaders as $headerName => $expectedValue) {
            self::assertArrayHasKey($headerName, $requestHeaders, "Missing header: $headerName");
            self::assertSame($expectedValue, $requestHeaders[$headerName], "Header $headerName has wrong value");
        }
    }

    public static function requestHeadersDataProvider(): Generator
    {
        yield 'authenticated endpoint includes auth token' => [
            'clientConfig' => ['token' => 'auth-token-123'],
            'method' => 'getJobStatus',
            'jobId' => 'job-123',
            'expectedHeaders' => [
                'Host' => ['query.test.keboola.com'],
                'User-Agent' => ['Keboola Query API PHP Client'],
                'X-StorageApi-Token' => ['auth-token-123'],
                'Content-type' => ['application/json'],
            ],
        ];

        yield 'runId header included when configured' => [
            'clientConfig' => ['token' => 'test-token', 'runId' => 'run-456'],
            'method' => 'getJobStatus',
            'jobId' => 'job-123',
            'expectedHeaders' => [
                'Host' => ['query.test.keboola.com'],
                'User-Agent' => ['Keboola Query API PHP Client'],
                'X-StorageApi-Token' => ['test-token'],
                'Content-type' => ['application/json'],
                'X-KBC-RunId' => ['run-456'],
            ],
        ];

        yield 'custom userAgent properly appended' => [
            'clientConfig' => ['token' => 'test-token', 'userAgent' => 'MyApp/2.0'],
            'method' => 'getJobStatus',
            'jobId' => 'job-123',
            'expectedHeaders' => [
                'Host' => ['query.test.keboola.com'],
                'User-Agent' => ['MyApp/2.0'],
                'X-StorageApi-Token' => ['test-token'],
                'Content-type' => ['application/json'],
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
