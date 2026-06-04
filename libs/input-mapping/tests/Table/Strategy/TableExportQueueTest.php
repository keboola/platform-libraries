<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\Local;
use Keboola\InputMapping\Table\Strategy\TableExportQueue;
use Keboola\InputMapping\Table\Strategy\TableLoadQueueInterface;
use PHPUnit\Framework\TestCase;

class TableExportQueueTest extends TestCase
{
    public function testGetJobIdsAndTables(): void
    {
        $table1 = $this->createTableOptions('table1');
        $table2 = $this->createTableOptions('table2');
        $exportJobs = [
            '111' => ['tableId' => 'in.c-main.t1', 'destination' => '/tmp/t1', 'exportOptions' => []],
            '222' => ['tableId' => 'in.c-main.t2', 'destination' => '/tmp/t2', 'exportOptions' => []],
        ];

        // Note: PHP silently casts numeric-string array keys to int, so '111' => becomes 111 =>.
        // We therefore use assertEquals (not assertSame) for job-id assertions to avoid int/string
        // type mismatch. The implementation intentionally does NOT cast keys — handleAsyncTasks()
        // can deal with both int and string ids.
        $queue = new TableExportQueue(['111' => $table1, '222' => $table2], Local::class, 'download', $exportJobs);

        self::assertInstanceOf(TableLoadQueueInterface::class, $queue);
        self::assertEquals(['111', '222'], $queue->getJobIds());
        self::assertSame([$table1, $table2], $queue->getAllTables());
        self::assertEquals($exportJobs, $queue->exportJobs);
        self::assertSame(Local::class, $queue->getStrategyClass());
        self::assertSame('download', $queue->getDestination());
    }

    public function testEmptyQueue(): void
    {
        $queue = new TableExportQueue([], Local::class, 'download');
        self::assertSame([], $queue->getJobIds());
        self::assertSame([], $queue->getAllTables());
        self::assertSame([], $queue->exportJobs);
        self::assertSame(Local::class, $queue->getStrategyClass());
        self::assertSame('download', $queue->getDestination());
    }

    private function createTableOptions(string $source): RewrittenInputTableOptions
    {
        return new RewrittenInputTableOptions(
            ['source' => $source, 'destination' => $source],
            $source,
            123,
            ['id' => $source, 'bucket' => ['backend' => 'snowflake'], 'isAlias' => false],
        );
    }
}
