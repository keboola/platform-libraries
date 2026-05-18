<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\WorkspaceJobType;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadJob;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadQueue;
use PHPUnit\Framework\TestCase;

class WorkspaceLoadQueueTest extends TestCase
{
    public function testGetJobIds(): void
    {
        $job1 = new WorkspaceLoadJob('job-123', WorkspaceJobType::CLONE, []);
        $job2 = new WorkspaceLoadJob('job-456', WorkspaceJobType::LOAD, []);
        $job3 = new WorkspaceLoadJob('job-789', WorkspaceJobType::CLONE, []);
        $jobs = [$job1, $job2, $job3];

        $queue = new WorkspaceLoadQueue($jobs);

        $result = $queue->getJobIds();

        self::assertSame(['job-123', 'job-456', 'job-789'], $result);
        self::assertCount(3, $result);
    }

    public function testGetAllTablesReturnsTablesFromMultipleJobs(): void
    {
        $table1 = $this->createMockTableOptions('table1');
        $table2 = $this->createMockTableOptions('table2');
        $table3 = $this->createMockTableOptions('table3');
        $table4 = $this->createMockTableOptions('table4');

        $job1 = new WorkspaceLoadJob('job-123', WorkspaceJobType::CLONE, [$table1, $table2]);
        $job2 = new WorkspaceLoadJob('job-456', WorkspaceJobType::LOAD, [$table3]);
        $job3 = new WorkspaceLoadJob('job-789', WorkspaceJobType::CLONE, [$table4]);

        $queue = new WorkspaceLoadQueue([$job1, $job2, $job3]);

        $result = $queue->getAllTables();

        // Should merge tables from all jobs in order
        self::assertSame([$table1, $table2, $table3, $table4], $result);
        self::assertCount(4, $result);
    }

    private function createMockTableOptions(string $tableName): RewrittenInputTableOptions
    {
        return new RewrittenInputTableOptions(
            [
                'source' => "in.c-test-bucket.{$tableName}",
                'destination' => $tableName,
            ],
            "in.c-test-bucket.{$tableName}",
            123,
            [
                'id' => "in.c-test-bucket.{$tableName}",
                'name' => $tableName,
                'displayName' => "Test {$tableName}",
                'columns' => ['col1', 'col2'],
                'columnMetadata' => [
                    'col1' => [['key' => 'KBC.datatype.type', 'value' => 'VARCHAR']],
                    'col2' => [['key' => 'KBC.datatype.type', 'value' => 'INTEGER']],
                ],
                'lastImportDate' => '2023-01-01 10:00:00',
            ],
        );
    }
}
