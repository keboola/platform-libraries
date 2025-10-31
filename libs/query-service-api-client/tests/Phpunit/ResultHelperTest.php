<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit;

use GuzzleHttp\Psr7\Response;
use Keboola\QueryApi\Response\JobResultsResponse;
use Keboola\QueryApi\Response\Statement;
use Keboola\QueryApi\ResultHelper;
use PHPUnit\Framework\TestCase;

class ResultHelperTest extends TestCase
{
    public function testMapColumnNamesIntoData(): void
    {
        $responseData = [
            'columns' => [
                ['name' => 'id', 'type' => 'text'],
                ['name' => 'name', 'type' => 'text'],
                ['name' => 'city', 'type' => 'text'],
            ],
            'data' => [
                ['1', 'Alice', 'Prague'],
                ['2', 'Bob', 'Liberec'],
                ['3', 'Charlie', 'Brno', 'EXTRA'],
            ],
            'status' => 'completed',
            'numberOfRows' => 3,
            'rowsAffected' => 3,
        ];

        $response = new Response(200, [], json_encode($responseData) ?: '');
        $input = JobResultsResponse::fromResponse($response);

        $actual = ResultHelper::mapColumnNamesIntoData($input);

        self::assertEquals('completed', $actual->getStatus());
        self::assertEquals(3, $actual->getNumberOfRows());
        self::assertEquals(3, $actual->getRowsAffected());
        self::assertNull($actual->getMessage());

        // Columns should be preserved unchanged
        $expectedColumns = [
            ['name' => 'id', 'type' => 'text'],
            ['name' => 'name', 'type' => 'text'],
            ['name' => 'city', 'type' => 'text'],
        ];
        self::assertSame($expectedColumns, $actual->getColumns());

        // Data rows should be mapped by column names
        $expectedData = [
            ['id' => '1', 'name' => 'Alice', 'city' => 'Prague'],
            ['id' => '2', 'name' => 'Bob', 'city' => 'Liberec'],
            ['id' => '3', 'name' => 'Charlie', 'city' => 'Brno'],
        ];
        self::assertSame($expectedData, $actual->getData());
    }

    public function testExtractAllStatementErrorsSingle(): void
    {
        $statements = [
            new Statement([
                'id' => 'b0065ce1-00d5-4723-8897-0c5a902ae446',
                'queryId' => 'query-1',
                'sessionId' => 'session-1',
                'query' => 'SELECT 1',
                'status' => 'completed',
                'numberOfRows' => 1,
                'rowsAffected' => 0,
                'executedAt' => '2024-01-01T10:00:00Z',
                'completedAt' => '2024-01-01T10:00:10Z',
                'createdAt' => '2024-01-01T10:00:00Z',
            ]),
            new Statement([
                'id' => '4feb6663-c98c-401d-a875-5bb72438e2cc',
                'queryId' => 'query-2',
                'sessionId' => 'session-2',
                'query' => 'SELECT * FROM Cooties',
                'status' => 'failed',
                'numberOfRows' => 0,
                'rowsAffected' => 0,
                'executedAt' => '2024-01-01T10:00:00Z',
                'completedAt' => '2024-01-01T10:00:10Z',
                'createdAt' => '2024-01-01T10:00:00Z',
                'error' => 'COOTIES does not exist or not authorized',
            ]),
            new Statement([
                'id' => 'stmt-spacey',
                'queryId' => 'query-3',
                'sessionId' => 'session-3',
                'query' => 'a spacey query',
                'status' => 'failed',
                'numberOfRows' => 0,
                'rowsAffected' => 0,
                'executedAt' => '2024-01-01T10:00:00Z',
                'completedAt' => '2024-01-01T10:00:10Z',
                'createdAt' => '2024-01-01T10:00:00Z',
                'error' => '                  there is also a lot of space   ',
            ]),
            new Statement([
                'id' => 'a57bc659-c9f7-45d4-a011-d6f10cc0e757',
                'queryId' => 'query-4',
                'sessionId' => 'session-4',
                'query' => 'SELECT 2',
                'status' => 'notExecuted',
                'numberOfRows' => 0,
                'rowsAffected' => 0,
                'executedAt' => '2024-01-01T10:00:00Z',
                'completedAt' => '2024-01-01T10:00:10Z',
                'createdAt' => '2024-01-01T10:00:00Z',
            ]),
        ];

        $actual = ResultHelper::extractAllStatementErrors($statements);
        self::assertEquals(
            "COOTIES does not exist or not authorized\nthere is also a lot of space",
            $actual,
        );
    }

    public function testExtractAllStatementErrorsEmptyArray(): void
    {
        $actual = ResultHelper::extractAllStatementErrors([]);
        self::assertSame('Unknown error', $actual);
    }
}
