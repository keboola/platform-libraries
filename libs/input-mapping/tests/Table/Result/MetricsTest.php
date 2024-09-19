<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Result;

use Keboola\InputMapping\Table\Result\Metrics;
use Keboola\InputMapping\Table\Result\TableMetrics;
use PHPUnit\Framework\TestCase;

class MetricsTest extends TestCase
{
    public function testAccessors(): void
    {
        $jobResults = [
            [
                'tableId' => 'in.c-input-mapping-test.test',
                'metrics' => [
                    'outBytesUncompressed' => 123,
                    'outBytes' => 0,
                ],
            ],
            [
                'tableId' => 'in.c-input-mapping-test.test',
                'metrics' => [
                    'outBytesUncompressed' => 0,
                    'outBytes' => 321,
                ],
            ],
        ];
        $metrics = new Metrics($jobResults);
        self::assertEquals(
            [
                new TableMetrics($jobResults[0]),
                new TableMetrics($jobResults[1]),
            ],
            $metrics->getTableMetrics(),
        );
    }
}
