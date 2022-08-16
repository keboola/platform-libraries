<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Table\Result;

use Generator;
use Keboola\OutputMapping\Table\Result\TableMetrics;
use PHPUnit\Framework\TestCase;

class TableMetricsTest extends TestCase
{
    public function accessorsData(): Generator
    {
        yield [
            [
                'operationName' => 'tableImport',
                'status' => 'success',
                'tableId' => 'in.c-myBucket.tableImported',
                'metrics' => [
                    'inBytes' => 123,
                    'inBytesUncompressed' => 0,
                ]
            ],
            'in.c-myBucket.tableImported',
            123,
            0,
        ];

        yield [
            [
                'operationName' => 'tableCreate',
                'tableId' => null,
                'status' => 'success',
                'results' => [
                    'id' => 'in.c-myBucket.tableCreated',
                ],
                'metrics' => [
                    'inBytes' => 0,
                    'inBytesUncompressed' => 5,
                ]
            ],
            'in.c-myBucket.tableCreated',
            0,
            5,
        ];
    }

    /**
     * @dataProvider accessorsData
     */
    public function testAccessors(
        array $jobResult,
        string $expectedTableId,
        int $expectedCompressedBytes,
        int $expectedUncompressedBytes
    ): void {
        $tableMetrics = new TableMetrics($jobResult);

        self::assertSame($expectedCompressedBytes, $tableMetrics->getCompressedBytes());
        self::assertSame($expectedUncompressedBytes, $tableMetrics->getUncompressedBytes());
        self::assertSame($expectedTableId, $tableMetrics->getTableId());
    }
}
