<?php

namespace Keboola\InputMapping\Tests\Table\Result;

use Keboola\OutputMapping\Table\Result\Metrics;
use Keboola\OutputMapping\Table\Result\TableMetrics;
use PHPUnit\Framework\TestCase;

class MetricsTest extends TestCase
{
    public function testAccessors()
    {
        $jobResults = [
            [
                'tableId' => 'in.c-output-mapping-test.test',
                'metrics' => [
                    'inBytesUncompressed' => 123,
                    'inBytes' => 0,
                ],
            ],
            [
                'tableId' => 'in.c-output-mapping-test.test',
                'metrics' => [
                    'inBytesUncompressed' => 0,
                    'inBytes' => 321,
                ],
            ],
        ];
        $metrics = new Metrics($jobResults);
        self::assertEquals(
            [
                new TableMetrics($jobResults[0]),
                new TableMetrics($jobResults[1]),
            ],
            iterator_to_array($metrics->getTableMetrics())
        );
    }
}
