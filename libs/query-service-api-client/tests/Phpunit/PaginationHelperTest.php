<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use Keboola\QueryApi\Client;
use Keboola\QueryApi\ClientException;
use Keboola\QueryApi\PaginationHelper;
use Keboola\QueryApi\Response\JobStatusResponse;
use PHPUnit\Framework\TestCase;

class PaginationHelperTest extends TestCase
{
    private function createClient(MockHandler $mockHandler): Client
    {
        return new Client(
            ['url' => 'https://query.test.keboola.com', 'token' => 'test-token'],
            ['handler' => HandlerStack::create($mockHandler)],
        );
    }

    /**
     * @param array<int, array<string, string>> $statements
     */
    private function createJobStatus(string $queryJobId, array $statements): JobStatusResponse
    {
        return JobStatusResponse::fromResponse(new Response(200, [], (string) json_encode([
            'queryJobId' => $queryJobId,
            'status' => 'completed',
            'actorType' => 'user',
            'createdAt' => '2024-01-01T00:00:00Z',
            'changedAt' => '2024-01-01T00:00:00Z',
            'statements' => $statements,
        ])));
    }

    /**
     * @param array<int, array<int, int>> $data
     */
    private function resultsResponse(array $data): Response
    {
        return new Response(200, [], (string) json_encode([
            'data' => $data,
            'status' => 'completed',
            'numberOfRows' => count($data),
            'rowsAffected' => 0,
            'columns' => [['name' => 'id']],
        ]));
    }

    public function testSingleStatementSinglePage(): void
    {
        $mockHandler = new MockHandler([
            $this->resultsResponse([[1], [2], [3]]),
        ]);

        $jobStatus = $this->createJobStatus('job-1', [
            ['id' => 'stmt-1', 'status' => 'completed', 'query' => 'SELECT 1'],
        ]);

        $helper = new PaginationHelper($this->createClient($mockHandler));
        $results = $helper->getAllResults($jobStatus, 500);

        self::assertCount(1, $results);
        self::assertArrayHasKey('stmt-1', $results);
        self::assertSame([[1], [2], [3]], $results['stmt-1']->getData());
        self::assertSame(3, $results['stmt-1']->getNumberOfRows());
        // Verify exactly one HTTP request (MockHandler empty = all responses consumed)
        self::assertSame(0, $mockHandler->count());
    }

    public function testSingleStatementMultiplePages(): void
    {
        $pageSize = 2;
        $mockHandler = new MockHandler([
            $this->resultsResponse([[1], [2]]),  // full page
            $this->resultsResponse([[3]]),        // partial page — last
        ]);

        $jobStatus = $this->createJobStatus('job-1', [
            ['id' => 'stmt-1', 'status' => 'completed', 'query' => 'SELECT 1'],
        ]);

        $helper = new PaginationHelper($this->createClient($mockHandler));
        $results = $helper->getAllResults($jobStatus, $pageSize);

        self::assertSame([[1], [2], [3]], $results['stmt-1']->getData());
        self::assertSame(3, $results['stmt-1']->getNumberOfRows());
        self::assertSame(0, $mockHandler->count());
    }

    public function testBoundaryExactMultipleOfPageSize(): void
    {
        // Total rows = 2 * pageSize — triggers one extra empty fetch before stopping
        $pageSize = 2;
        $mockHandler = new MockHandler([
            $this->resultsResponse([[1], [2]]),  // full page
            $this->resultsResponse([[3], [4]]),  // full page — triggers extra fetch
            $this->resultsResponse([]),           // empty page — loop stops
        ]);

        $jobStatus = $this->createJobStatus('job-1', [
            ['id' => 'stmt-1', 'status' => 'completed', 'query' => 'SELECT 1'],
        ]);

        $helper = new PaginationHelper($this->createClient($mockHandler));
        $results = $helper->getAllResults($jobStatus, $pageSize);

        self::assertSame([[1], [2], [3], [4]], $results['stmt-1']->getData());
        self::assertSame(4, $results['stmt-1']->getNumberOfRows());
        self::assertSame(0, $mockHandler->count()); // all 3 responses consumed
    }

    public function testMultipleStatements(): void
    {
        $mockHandler = new MockHandler([
            $this->resultsResponse([[1]]),        // stmt-1
            $this->resultsResponse([[2], [3]]),   // stmt-2
        ]);

        $jobStatus = $this->createJobStatus('job-1', [
            ['id' => 'stmt-1', 'status' => 'completed', 'query' => 'SELECT 1'],
            ['id' => 'stmt-2', 'status' => 'completed', 'query' => 'SELECT 2'],
        ]);

        $helper = new PaginationHelper($this->createClient($mockHandler));
        $results = $helper->getAllResults($jobStatus, 500);

        self::assertCount(2, $results);
        self::assertArrayHasKey('stmt-1', $results);
        self::assertArrayHasKey('stmt-2', $results);
        self::assertSame(1, $results['stmt-1']->getNumberOfRows());
        self::assertSame(2, $results['stmt-2']->getNumberOfRows());
    }

    public function testNonCompletedStatementsAreSkipped(): void
    {
        $mockHandler = new MockHandler([
            $this->resultsResponse([[1]]),  // only stmt-1; stmt-2 (failed) is not fetched
        ]);

        $jobStatus = $this->createJobStatus('job-1', [
            ['id' => 'stmt-1', 'status' => 'completed', 'query' => 'SELECT 1'],
            ['id' => 'stmt-2', 'status' => 'failed', 'query' => 'SELECT 2'],
        ]);

        $helper = new PaginationHelper($this->createClient($mockHandler));
        $results = $helper->getAllResults($jobStatus, 500);

        self::assertCount(1, $results);
        self::assertArrayHasKey('stmt-1', $results);
        self::assertArrayNotHasKey('stmt-2', $results);
    }

    public function testEmptyStatementResults(): void
    {
        $mockHandler = new MockHandler([
            $this->resultsResponse([]),
        ]);

        $jobStatus = $this->createJobStatus('job-1', [
            ['id' => 'stmt-1', 'status' => 'completed', 'query' => 'SELECT 1'],
        ]);

        $helper = new PaginationHelper($this->createClient($mockHandler));
        $results = $helper->getAllResults($jobStatus, 500);

        self::assertSame([], $results['stmt-1']->getData());
        self::assertSame(0, $results['stmt-1']->getNumberOfRows());
    }

    public function testNoStatements(): void
    {
        $mockHandler = new MockHandler([]); // no HTTP calls expected

        $jobStatus = $this->createJobStatus('job-1', []);

        $helper = new PaginationHelper($this->createClient($mockHandler));
        $results = $helper->getAllResults($jobStatus, 500);

        self::assertSame([], $results);
    }

    public function testClientExceptionPropagates(): void
    {
        // Client retries 5xx responses up to DEFAULT_BACKOFF_RETRIES (3) times,
        // so we need 4 responses total (1 original + 3 retries) to exhaust the retry logic.
        $errorResponse = new Response(500, [], (string) json_encode(['exception' => 'Internal Server Error']));
        $mockHandler = new MockHandler([
            $errorResponse,
            $errorResponse,
            $errorResponse,
            $errorResponse,
        ]);

        $jobStatus = $this->createJobStatus('job-1', [
            ['id' => 'stmt-1', 'status' => 'completed', 'query' => 'SELECT 1'],
        ]);

        $helper = new PaginationHelper($this->createClient($mockHandler));

        self::expectException(ClientException::class);
        $helper->getAllResults($jobStatus, 500);
    }
}
