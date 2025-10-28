<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit\Response;

use Generator;
use Keboola\QueryApi\ClientException;
use Keboola\QueryApi\Response\Statement;
use PHPUnit\Framework\TestCase;

class StatementTest extends TestCase
{
    public function testStatementCreationWithAllFields(): void
    {
        $data = [
            'id' => 'stmt-123',
            'queryId' => 'query-456', // Optional field, but set for testing
            'sessionId' => 'session-789',
            'query' => 'SELECT * FROM table',
            'status' => 'completed',
            'numberOfRows' => 10,
            'rowsAffected' => 0,
            'executedAt' => '2024-01-01T10:00:00Z',
            'completedAt' => '2024-01-01T10:00:10Z',
            'createdAt' => '2024-01-01T10:00:05Z',
            'error' => 'Some error',
        ];

        $statement = new Statement($data);

        self::assertEquals('stmt-123', $statement->getId());
        self::assertEquals('query-456', $statement->getQueryId());
        self::assertEquals('session-789', $statement->getSessionId());
        self::assertEquals('SELECT * FROM table', $statement->getQuery());
        self::assertEquals('completed', $statement->getStatus());
        self::assertSame(10, $statement->getNumberOfRows());
        self::assertSame(0, $statement->getRowsAffected());
        self::assertEquals('2024-01-01T10:00:00Z', $statement->getExecutedAt());
        self::assertEquals('2024-01-01T10:00:10Z', $statement->getCompletedAt());
        self::assertEquals('2024-01-01T10:00:05Z', $statement->getCreatedAt());
        self::assertEquals('Some error', $statement->getError());
    }

    public function testStatementCreationWithoutError(): void
    {
        $data = [
            'id' => 'stmt-123',
            'queryId' => 'query-456', // Optional field, but set for testing
            'sessionId' => 'session-789',
            'query' => 'SELECT * FROM table',
            'status' => 'completed',
            'numberOfRows' => 10,
            'rowsAffected' => 0,
            'executedAt' => '2024-01-01T10:00:00Z',
            'completedAt' => '2024-01-01T10:00:10Z',
            'createdAt' => '2024-01-01T10:00:05Z',
        ];

        $statement = new Statement($data);

        self::assertNull($statement->getError());
    }

    public function testStatementCreationWithoutQueryId(): void
    {
        $data = [
            'id' => 'stmt-999',
            'sessionId' => 'session-999',
            'query' => 'SELECT 1',
            'status' => 'completed',
            'numberOfRows' => 1,
            'rowsAffected' => 0,
            'executedAt' => '2024-01-01T10:00:05Z',
            'completedAt' => '2024-01-01T10:00:10Z',
            'createdAt' => '2024-01-01T10:00:00Z',
        ];

        $statement = new Statement($data);

        self::assertEquals('stmt-999', $statement->getId());
        self::assertNull($statement->getQueryId());
    }

    /**
     * @param array<string, mixed> $data
     * @dataProvider missingFieldDataProvider
     */
    public function testStatementThrowsExceptionForMissingField(array $data, string $expectedField): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage("Invalid statement response: missing $expectedField");

        new Statement($data);
    }


    public static function missingFieldDataProvider(): Generator
    {
        $requiredFields = [
            'id' => 'stmt-123',
            'query' => 'SELECT * FROM table',
            'status' => 'completed',
        ];

        foreach (array_keys($requiredFields) as $fieldToRemove) {
            $incompleteData = $requiredFields;
            unset($incompleteData[$fieldToRemove]);
            yield "missing $fieldToRemove" => [$incompleteData, $fieldToRemove];
        }
    }
}
