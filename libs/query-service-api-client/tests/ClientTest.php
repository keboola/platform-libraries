<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests;

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

        self::assertEquals(['queryJobId' => 'job-12345'], $result);
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

        self::assertEquals('job-12345', $result['queryJobId']);
        self::assertEquals('running', $result['status']);
    }

    public function testCancelJob(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode(['queryJobId' => 'job-12345']) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->cancelJob('job-12345', ['reason' => 'User requested']);

        self::assertEquals(['queryJobId' => 'job-12345'], $result);
    }

    public function testGetJobResults(): void
    {
        $mockHandler = new MockHandler([
            new Response(200, [], json_encode([
                'data' => [['id' => 1, 'name' => 'test']],
                'status' => 'completed',
                'rowsAffected' => 1,
            ]) ?: ''),
        ]);

        $client = $this->createClientWithMockHandler($mockHandler);

        $result = $client->getJobResults('job-12345', 'stmt-67890');

        self::assertEquals('completed', $result['status']);
        self::assertEquals(1, $result['rowsAffected']);
        assert(is_array($result['data']));
        self::assertCount(1, $result['data']);
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

        self::assertEquals('query', $result['service']);
        self::assertEquals('ok', $result['status']);
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

        self::assertEquals('query', $result['service']);
        self::assertEquals('ok', $result['status']);
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
