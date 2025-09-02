<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests\Phpunit;

use Keboola\QueryApi\ResultHelper;
use PHPUnit\Framework\TestCase;

class ResultHelperTest extends TestCase
{
    public function testMapColumnNamesIntoData(): void
    {
        $input = [
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
        ];

        $expected = [
            'columns' => [
                ['name' => 'id', 'type' => 'text'],
                ['name' => 'name', 'type' => 'text'],
                ['name' => 'city', 'type' => 'text'],
            ],
            'data' => [
                ['id' => '1', 'name' => 'Alice', 'city' => 'Prague'],
                ['id' => '2', 'name' => 'Bob', 'city' => 'Liberec'],
                ['id' => '3', 'name' => 'Charlie', 'city' => 'Brno'],
            ],
        ];

        $actual = ResultHelper::mapColumnNamesIntoData($input);

        // Columns should be preserved unchanged
        self::assertSame($expected['columns'], $actual['columns']);
        // Data rows should be mapped by column names
        self::assertSame($expected['data'], $actual['data']);
    }

    public function testExtractAllStatementErrorsSingle(): void
    {
        $responseData = [
            'queryJobId' => '74001bf0-c79c-49f7-bad9-ef96db5ff28e',
            'status' => 'failed',
            'statements' => [
                [
                    'id' => 'b0065ce1-00d5-4723-8897-0c5a902ae446',
                    'query' => 'SELECT 1',
                    'status' => 'completed',
                ],
                [
                    'id' => '4feb6663-c98c-401d-a875-5bb72438e2cc',
                    'query' => 'SELECT * FROM Cooties',
                    'status' => 'failed',
                    'error' => 'COOTIES does not exist or not authorized',
                ],
                [
                    'id' => '4feb6663-c98c-401d-a875-5bb72438e2cc',
                    'query' => 'a spacey query',
                    'status' => 'failed',
                    'error' => '                  there is also a lot of space   ',
                ],
                [
                    'id' => 'a57bc659-c9f7-45d4-a011-d6f10cc0e757',
                    'query' => 'SELECT 2',
                    'status' => 'notExecuted',
                ],
            ],
        ];

        $actual = ResultHelper::extractAllStatementErrors($responseData);
        self::assertEquals(
            "COOTIES does not exist or not authorized\nthere is also a lot of space",
            $actual,
        );
    }

    public function testExtractAllStatementErrorsInvalidArray(): void
    {
        $actual = ResultHelper::extractAllStatementErrors([]);
        self::assertSame('Unknown error', $actual);
    }
}
