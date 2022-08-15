<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Table\Result;

use Keboola\OutputMapping\Table\Result\TableMetrics;
use PHPUnit\Framework\TestCase;

class TableMetricsTest extends TestCase
{
    public function testAccessors(): void
    {
        $tableMetrics = new TableMetrics(
            [
                'tableId' => 'in.c-output-mapping-test.test',
                'metrics' => [
                    'inBytesUncompressed' => 123,
                    'inBytes' => 5,
                ],
            ]
        );
        self::assertSame(5, $tableMetrics->getCompressedBytes());
        self::assertSame(123, $tableMetrics->getUncompressedBytes());
        self::assertSame('in.c-output-mapping-test.test', $tableMetrics->getTableId());
    }
}
