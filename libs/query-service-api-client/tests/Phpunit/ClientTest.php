<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit;

use GuzzleHttp\Psr7\Response;
use Keboola\ApiClientBase\Json;
use Keboola\QueryApi\Client;
use Keboola\QueryApi\Exception\ClientException;
use Keboola\QueryApi\Tests\ApiClientTestTrait;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class ClientTest extends TestCase
{
    use ApiClientTestTrait;

    private const BASE_URL = 'https://query.test.keboola.com';
    private const TOKEN = 'test-token';

    public function testConstructorRejectsEmptyBaseUrl(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Base URL must be a non-empty string');

        new Client('', self::TOKEN); // @phpstan-ignore-line
    }

    public function testConstructorRejectsEmptyToken(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Storage API token must not be empty');

        new Client(self::BASE_URL, ''); // @phpstan-ignore-line
    }

    public function testSubmitQueryJob(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(201, ['Content-Type' => 'application/json'], Json::encodeArray(['queryJobId' => 'job-12345'])),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->submitQueryJob('main', 'workspace-123', [
            'statements' => ['SELECT * FROM table1'],
            'transactional' => true,
        ]);

        self::assertSame('job-12345', $response->getQueryJobId());
        self::assertCount(1, $history);
        self::assertRequestEquals(
            'POST',
            self::BASE_URL . '/api/v1/branches/main/workspaces/workspace-123/queries',
            ['Content-Type' => 'application/json', 'X-StorageApi-Token' => self::TOKEN],
            Json::encodeArray(['statements' => ['SELECT * FROM table1'], 'transactional' => true]),
            $history[0]['request'],
        );
    }

    public function testGetJobStatus(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([
                'queryJobId' => 'job-12345',
                'status' => 'running',
                'actorType' => 'user',
                'createdAt' => '2024-01-01T00:00:00Z',
                'changedAt' => '2024-01-01T00:00:00Z',
                'statements' => [],
            ])),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->getJobStatus('job-12345');

        self::assertSame('job-12345', $response->getQueryJobId());
        self::assertSame('running', $response->getStatus());
        self::assertSame([], $response->getStatements());
        self::assertSame(
            self::BASE_URL . '/api/v1/queries/job-12345',
            $history[0]['request']->getUri()->__toString(),
        );
        self::assertSame(self::TOKEN, $history[0]['request']->getHeaderLine('X-StorageApi-Token'));
    }

    public function testCancelJob(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray(['queryJobId' => 'job-12345'])),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->cancelJob('job-12345', ['reason' => 'User requested']);

        self::assertSame('job-12345', $response->getQueryJobId());
        self::assertRequestEquals(
            'POST',
            self::BASE_URL . '/api/v1/queries/job-12345/cancel',
            ['Content-Type' => 'application/json', 'X-StorageApi-Token' => self::TOKEN],
            Json::encodeArray(['reason' => 'User requested']),
            $history[0]['request'],
        );
    }

    public function testGetJobResults(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([
                'data' => [['id' => 1, 'name' => 'test']],
                'status' => 'completed',
                'numberOfRows' => 1,
                'rowsAffected' => 1,
            ])),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->getJobResults('job-12345', 'stmt-67890');

        self::assertSame('completed', $response->getStatus());
        self::assertSame(1, $response->getRowsAffected());
        self::assertSame(
            self::BASE_URL . '/api/v1/queries/job-12345/stmt-67890/results',
            $history[0]['request']->getUri()->__toString(),
        );
    }

    public function testGetJobResultsWithQueryParams(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([
                'data' => [], 'status' => 'completed', 'numberOfRows' => 0, 'rowsAffected' => 0,
            ])),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $client->getJobResults('job-12345', 'stmt-67890', 100, 200);

        $query = $history[0]['request']->getUri()->getQuery();
        self::assertStringContainsString('pageSize=100', $query);
        self::assertStringContainsString('offset=200', $query);
    }

    public function testGetJobResultsWithoutQueryParams(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray([
                'data' => [], 'status' => 'completed', 'numberOfRows' => 0, 'rowsAffected' => 0,
            ])),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $client->getJobResults('job-12345', 'stmt-67890');

        self::assertSame('', $history[0]['request']->getUri()->getQuery());
    }

    public function testRunIdHeaderSentWhenConfigured(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], self::statusBody()),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, runId: 'run-456', requestHandler: $requestHandler(...));

        $client->getJobStatus('job-1');

        self::assertSame('run-456', $history[0]['request']->getHeaderLine('X-KBC-RunId'));
    }

    public function testRunIdHeaderAbsentByDefault(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], self::statusBody()),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $client->getJobStatus('job-1');

        self::assertFalse($history[0]['request']->hasHeader('X-KBC-RunId'));
    }

    public function testCustomUserAgent(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], self::statusBody()),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, userAgent: 'MyApp/2.0', requestHandler: $requestHandler(...));

        $client->getJobStatus('job-1');

        self::assertSame('MyApp/2.0', $history[0]['request']->getHeaderLine('User-Agent'));
    }

    public function testDefaultUserAgent(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(200, ['Content-Type' => 'application/json'], self::statusBody()),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $client->getJobStatus('job-1');

        self::assertSame('Keboola Query API PHP Client', $history[0]['request']->getHeaderLine('User-Agent'));
    }

    public function testServerErrorIsRetriedThenSucceeds(): void
    {
        $requestHandler = self::createRequestHandler($history, [
            new Response(500),
            new Response(200, ['Content-Type' => 'application/json'], Json::encodeArray(['queryJobId' => 'job-1'])),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, requestHandler: $requestHandler(...));

        $response = $client->cancelJob('job-1');

        self::assertSame('job-1', $response->getQueryJobId());
        self::assertCount(2, $history);
    }

    public function testClientErrorUsesExceptionFieldMessage(): void
    {
        $body = Json::encodeArray(['exception' => 'Invalid job ID format']);
        $requestHandler = self::createRequestHandler($history, [
            new Response(400, ['Content-Type' => 'application/json'], $body),
        ]);
        $client = new Client(self::BASE_URL, self::TOKEN, backoffMaxTries: 0, requestHandler: $requestHandler(...));

        try {
            $client->getJobStatus('bad-id');
            self::fail('Expected ClientException');
        } catch (ClientException $e) {
            self::assertSame('Invalid job ID format', $e->getMessage());
            self::assertSame(400, $e->getCode());
            self::assertSame(400, $e->getStatusCode());
            self::assertSame($body, $e->getResponseBody());
        }
    }

    private static function statusBody(): string
    {
        return Json::encodeArray([
            'queryJobId' => 'job-1',
            'status' => 'running',
            'actorType' => 'user',
            'createdAt' => '2024-01-01T00:00:00Z',
            'changedAt' => '2024-01-01T00:00:00Z',
            'statements' => [],
        ]);
    }
}
