<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use RuntimeException;

/**
 * Functional tests for executeTableLoadsToWorkspace (beta path)
 * This tests the new single-job workspace loading optimization used by SQL editor API
 */
class ExecuteTableLoadsWorkspaceSnowflakeTest extends AbstractTestCase
{
    private function createStrategy(ClientWrapper $clientWrapper): AbstractWorkspaceStrategy
    {
        $stagingFactory = $this->getWorkspaceStagingFactory(
            clientWrapper: $clientWrapper,
            logger: $this->testLogger,
        );

        $strategy = $stagingFactory->getTableInputStrategy(
            $this->temp->getTmpFolder(),
            new InputTableStateList([]),
        );

        if (!$strategy instanceof AbstractWorkspaceStrategy) {
            throw new RuntimeException('Expected AbstractWorkspaceStrategy');
        }

        return $strategy;
    }

    #[NeedsTestTables(3), NeedsEmptyOutputBucket]
    public function testMixedOperationsWithSingleJob(): void
    {
        $clientWrapper = $this->initClient();
        $runId = $clientWrapper->getBasicClient()->generateRunId();
        $clientWrapper->getBranchClient()->setRunId($runId);

        $strategy = $this->createStrategy($clientWrapper);
        $sourceBranchId = (int) $clientWrapper->getBranchId();

        // Create mixed operations: 2 clones, 1 copy
        $cloneTable1 = new RewrittenInputTableOptions(
            [
                'source' => $this->firstTableId,
                'destination' => 'clone1',
            ],
            $this->firstTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId),
        );

        $cloneTable2 = new RewrittenInputTableOptions(
            [
                'source' => $this->thirdTableId,
                'destination' => 'clone2',
                'keep_internal_timestamp_column' => false,
            ],
            $this->thirdTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->thirdTableId),
        );

        $copyTable = new RewrittenInputTableOptions(
            [
                'source' => $this->secondTableId,
                'destination' => 'copy1',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
            ],
            $this->secondTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->secondTableId),
        );

        // Execute using beta path
        $queue = $strategy->prepareAndExecuteTableLoads(
            [$cloneTable1, $copyTable, $cloneTable2],
            false, // preserve=false (clean workspace)
        );

        // KEY ASSERTION: Single job (not 2)
        self::assertCount(1, $queue->jobs);
        $job = $queue->jobs[0];
        self::assertCount(3, $job->tables);

        // Wait for job completion
        $clientWrapper->getBranchClient()->handleAsyncTasks([$job->jobId]);

        // Verify tables loaded correctly in workspace
        try {
            $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'clone1', 'name' => 'clone1'],
            );
            self::fail('Must throw exception for _timestamp column');
        } catch (ClientException $e) {
            self::assertStringContainsString('Invalid columns: _timestamp:', $e->getMessage());
        }

        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'copy1', 'name' => 'copy1'],
        );
        self::assertTrue($clientWrapper->getTableAndFileStorageClient()->tableExists(
            $this->emptyOutputBucketId . '.copy1',
        ));

        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'clone2', 'name' => 'clone2'],
        );
        self::assertTrue($clientWrapper->getTableAndFileStorageClient()->tableExists(
            $this->emptyOutputBucketId . '.clone2',
        ));

        // Verify log messages
        self::assertTrue($this->testHandler->hasInfoThatContains('Cleaning workspace and loading tables.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Cloning 2 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 1 tables to workspace.'));
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testVerifyJobHasLoadTypeParameters(): void
    {
        $clientWrapper = $this->initClient();
        $strategy = $this->createStrategy($clientWrapper);
        $sourceBranchId = (int) $clientWrapper->getBranchId();

        $cloneTable = new RewrittenInputTableOptions(
            ['source' => $this->firstTableId, 'destination' => 'test_clone'],
            $this->firstTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId),
        );

        $copyTable = new RewrittenInputTableOptions(
            [
                'source' => $this->secondTableId,
                'destination' => 'test_copy',
                'where_column' => 'Id',
                'where_values' => ['id2'],
            ],
            $this->secondTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->secondTableId),
        );

        $runId = $clientWrapper->getBasicClient()->generateRunId();
        $clientWrapper->getBranchClient()->setRunId($runId);

        $queue = $strategy->prepareAndExecuteTableLoads([$cloneTable, $copyTable], false);

        // Wait for job completion
        $clientWrapper->getBranchClient()->handleAsyncTasks([$queue->jobs[0]->jobId]);

        // Verify job format in API
        sleep(5);
        $allJobs = $clientWrapper->getTableAndFileStorageClient()->listJobs(['limit' => 200]);

        // Filter jobs by runId
        $runJobs = [];
        foreach ($allJobs as $job) {
            if ($job['runId'] === $runId) {
                $runJobs[] = $job;
            }
        }

        // KEY ASSERTION: Only 1 job created (not 2 like old API)
        self::assertCount(1, $runJobs);

        $workspaceLoadJob = reset($runJobs);
        self::assertSame('workspaceLoad', $workspaceLoadJob['operationName']);
        self::assertCount(2, $workspaceLoadJob['operationParams']['input']);

        // KEY ASSERTIONS: Verify loadType parameters (new API format)
        $input1 = $workspaceLoadJob['operationParams']['input'][0];
        self::assertArrayHasKey('loadType', $input1);
        self::assertSame('CLONE', $input1['loadType']);
        self::assertSame('test_clone', $input1['destination']);

        $input2 = $workspaceLoadJob['operationParams']['input'][1];
        self::assertArrayHasKey('loadType', $input2);
        self::assertSame('COPY', $input2['loadType']);
        self::assertSame('test_copy', $input2['destination']);
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testCleanWorkspaceWithPreserveFalse(): void
    {
        $clientWrapper = $this->initClient();
        $strategy = $this->createStrategy($clientWrapper);
        $sourceBranchId = (int) $clientWrapper->getBranchId();

        // First load: create initial table
        $initialTable = new RewrittenInputTableOptions(
            ['source' => $this->firstTableId, 'destination' => 'initial_table'],
            $this->firstTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId),
        );

        $queue = $strategy->prepareAndExecuteTableLoads([$initialTable], true);
        $clientWrapper->getBranchClient()->handleAsyncTasks([$queue->jobs[0]->jobId]);

        // Second load: preserve=false should clean workspace
        $newTable = new RewrittenInputTableOptions(
            [
                'source' => $this->secondTableId,
                'destination' => 'new_table',
                'keep_internal_timestamp_column' => false,
            ],
            $this->secondTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->secondTableId),
        );

        $queue = $strategy->prepareAndExecuteTableLoads([$newTable], false);

        // KEY ASSERTION: Single job with preserve=0 (clean + load)
        self::assertCount(1, $queue->jobs);

        $clientWrapper->getBranchClient()->handleAsyncTasks([$queue->jobs[0]->jobId]);

        // Verify initial_table is gone (workspace was cleaned)
        try {
            $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                [
                    'dataWorkspaceId' => $this->workspaceId,
                    'dataTableName' => 'initial_table',
                    'name' => 'initial_table',
                ],
            );
            self::fail('Should throw 404 for table not found');
        } catch (ClientException $e) {
            self::assertStringContainsString('Table "initial_table" not found', $e->getMessage());
        }

        // Verify new_table exists
        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'new_table', 'name' => 'new_table'],
        );
        self::assertTrue($clientWrapper->getTableAndFileStorageClient()->tableExists(
            $this->emptyOutputBucketId . '.new_table',
        ));
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testPreserveWorkspaceWithPreserveTrue(): void
    {
        $clientWrapper = $this->initClient();
        $strategy = $this->createStrategy($clientWrapper);
        $sourceBranchId = (int) $clientWrapper->getBranchId();

        // First load
        $firstTable = new RewrittenInputTableOptions(
            [
                'source' => $this->firstTableId,
                'destination' => 'first_table',
                'keep_internal_timestamp_column' => false,
            ],
            $this->firstTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId),
        );

        $queue = $strategy->prepareAndExecuteTableLoads([$firstTable], false);
        $clientWrapper->getBranchClient()->handleAsyncTasks([$queue->jobs[0]->jobId]);

        // Second load with preserve=true
        $secondTable = new RewrittenInputTableOptions(
            [
                'source' => $this->secondTableId,
                'destination' => 'second_table',
                'keep_internal_timestamp_column' => false,
            ],
            $this->secondTableId,
            $sourceBranchId,
            $clientWrapper->getTableAndFileStorageClient()->getTable($this->secondTableId),
        );

        $queue = $strategy->prepareAndExecuteTableLoads([$secondTable], true);

        // KEY ASSERTION: Single job with preserve=1 (load only, no clean)
        self::assertCount(1, $queue->jobs);

        $clientWrapper->getBranchClient()->handleAsyncTasks([$queue->jobs[0]->jobId]);

        // Verify first_table still exists (workspace was preserved)
        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'first_table', 'name' => 'first_table'],
        );
        self::assertTrue($clientWrapper->getTableAndFileStorageClient()->tableExists(
            $this->emptyOutputBucketId . '.first_table',
        ));

        // Verify second_table also exists
        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'second_table', 'name' => 'second_table'],
        );
        self::assertTrue($clientWrapper->getTableAndFileStorageClient()->tableExists(
            $this->emptyOutputBucketId . '.second_table',
        ));
    }
}
