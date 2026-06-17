<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\Snowflake;
use Keboola\InputMapping\Table\Strategy\TableLoadQueueInterface;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadJob;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadQueue;
use PHPUnit\Framework\TestCase;

class WorkspaceLoadQueueTest extends TestCase
{
    public function testGetJobIdsAndTables(): void
    {
        $table1 = $this->createTableOptions('table1');
        $table2 = $this->createTableOptions('table2');

        $queue = new WorkspaceLoadQueue([
            new WorkspaceLoadJob('123', [$table1]),
            new WorkspaceLoadJob('456', [$table2]),
        ], Snowflake::class, 'destination');

        self::assertInstanceOf(TableLoadQueueInterface::class, $queue);
        self::assertSame(['123', '456'], $queue->getJobIds());
        self::assertSame([$table1, $table2], $queue->getAllTables());
        self::assertSame(Snowflake::class, $queue->getStrategyClass());
        self::assertSame('destination', $queue->getDestination());
    }

    public function testEmptyQueue(): void
    {
        $queue = new WorkspaceLoadQueue([], Snowflake::class, 'destination');
        self::assertSame([], $queue->getJobIds());
        self::assertSame([], $queue->getAllTables());
        self::assertSame(Snowflake::class, $queue->getStrategyClass());
        self::assertSame('destination', $queue->getDestination());
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
