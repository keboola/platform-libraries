<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Result;

use Keboola\InputMapping\Table\Result\TableMetrics;
use PHPUnit\Framework\TestCase;

class TableMetricsTest extends TestCase
{
    public function testAccessors(): void
    {
        $tableMetrics = new TableMetrics(
            [
                'tableId' => 'in.c-input-mapping-test.test',
                'metrics' => [
                    'outBytesUncompressed' => 123,
                    'outBytes' => 0,
                ],
            ]
        );
        self::assertSame(0, $tableMetrics->getCompressedBytes());
        self::assertSame(123, $tableMetrics->getUncompressedBytes());
        self::assertSame('in.c-input-mapping-test.test', $tableMetrics->getTableId());
    }
}
