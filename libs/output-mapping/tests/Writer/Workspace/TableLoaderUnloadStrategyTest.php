<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\Workspaces;
use RuntimeException;

class TableLoaderUnloadStrategyTest extends AbstractTestCase
{
    #[NeedsEmptyOutputBucket]
    #[NeedsTestTables(2)]
    public function testDirectGrantSkipsTableImport(): void
    {
        $this->initWorkspace();
        $this->prepareWorkspaceWithTables($this->testBucketId);

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1',
                'unload_strategy' => 'direct-grant',
            ],
        ];

        $systemMetadata = new SystemMetadata([
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '1234',
            'runId' => '1234567',
        ]);

        $strategyFactory = $this->getWorkspaceStagingFactory(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
        );

        $tableLoader = $this->getTableLoader(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            strategyFactory: $strategyFactory,
        );

        $storageApiToken = $this->clientWrapper->getToken();

        $configuration = new OutputMappingSettings(
            [
                'mapping' => $configs,
            ],
            '',
            $storageApiToken,
            false,
            OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
        );

        $result = $tableLoader->uploadTables($configuration, $systemMetadata);
        $result->waitForAll();

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(0, $tables, 'No tables should be imported when unload_strategy is direct-grant');
    }

    #[NeedsEmptyOutputBucket]
    #[NeedsTestTables]
    public function testDirectGrantOnlyAppliesToWorkspaceStrategy(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table1.csv', "id,name\n1,test\n");

        $configs = [
            [
                'source' => 'table1.csv',
                'destination' => $this->emptyOutputBucketId . '.table1',
                'unload_strategy' => 'direct-grant',
            ],
        ];

        $systemMetadata = new SystemMetadata([
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '1234',
            'runId' => '1234567',
        ]);

        $strategyFactory = $this->getLocalStagingFactory(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            stagingPath: $root . '/upload',
        );

        $tableLoader = $this->getTableLoader(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            strategyFactory: $strategyFactory,
        );

        $storageApiToken = $this->clientWrapper->getToken();

        $configuration = new OutputMappingSettings(
            [
                'mapping' => $configs,
            ],
            '',
            $storageApiToken,
            false,
            OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
        );

        $result = $tableLoader->uploadTables($configuration, $systemMetadata);
        $result->waitForAll();

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(
            1,
            $tables,
            'Table should be imported for local strategy even with unload_strategy=direct-grant',
        );
        self::assertEquals($this->emptyOutputBucketId . '.table1', $tables[0]['id']);
    }

    #[NeedsEmptyOutputBucket]
    #[NeedsTestTables(2)]
    public function testNoUnloadWhenNoDirectGrantMappings(): void
    {
        $this->initWorkspace();
        $this->prepareWorkspaceWithTables($this->testBucketId);

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1',
            ],
        ];

        $systemMetadata = new SystemMetadata([
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '1234',
            'runId' => '1234567',
        ]);

        $strategyFactory = $this->getWorkspaceStagingFactory(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
        );

        $tableLoader = $this->getTableLoader(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            strategyFactory: $strategyFactory,
        );

        $storageApiToken = $this->clientWrapper->getToken();

        $configuration = new OutputMappingSettings(
            [
                'mapping' => $configs,
            ],
            '',
            $storageApiToken,
            false,
            OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
        );

        $result = $tableLoader->uploadTables($configuration, $systemMetadata);
        self::assertSame(1, $result->getTaskCount(), 'Only the table load job is queued, no metadata refresh');
        $result->waitForAll();

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables, 'Table should be imported when unload_strategy is not set');
        self::assertEquals($this->emptyOutputBucketId . '.table1', $tables[0]['id']);
    }

    #[NeedsEmptyOutputBucket]
    #[NeedsTestTables(2)]
    public function testMixedMappingsWithAndWithoutDirectGrant(): void
    {
        $this->initWorkspace();
        $this->prepareWorkspaceWithTables($this->testBucketId);

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1',
                'unload_strategy' => 'direct-grant',
            ],
            [
                'source' => 'table2a',
                'destination' => $this->emptyOutputBucketId . '.table2',
            ],
        ];

        $systemMetadata = new SystemMetadata([
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '1234',
            'runId' => '1234567',
        ]);

        $strategyFactory = $this->getWorkspaceStagingFactory(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
        );

        $tableLoader = $this->getTableLoader(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            strategyFactory: $strategyFactory,
        );

        $storageApiToken = $this->clientWrapper->getToken();

        $configuration = new OutputMappingSettings(
            [
                'mapping' => $configs,
            ],
            '',
            $storageApiToken,
            false,
            OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
        );

        $result = $tableLoader->uploadTables($configuration, $systemMetadata);
        $result->waitForAll();

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables, 'Only non-direct-grant table should be imported');
        self::assertEquals($this->emptyOutputBucketId . '.table2', $tables[0]['id']);
    }

    #[NeedsEmptyOutputBucket]
    public function testDirectGrantMetadataRefreshIsAwaited(): void
    {
        $destinationTableId = $this->emptyOutputBucketId . '.table1';
        $this->initDirectGrantWorkspace($destinationTableId);

        $tableQueue = $this->uploadDirectGrantTable($destinationTableId);

        self::assertSame(1, $tableQueue->getTaskCount(), 'The metadata refresh job is the only queued job');
        self::assertSame([], $tableQueue->getLoadTableTasks(), 'A direct-grant table has no table load job');

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds, 'The refresh job of the workspace is awaited');

        $job = $this->clientWrapper->getBranchClient()->getJob((int) $jobIds[0]);
        self::assertSame('refreshStorageBuckets', $job['operationName']);
    }

    #[NeedsEmptyOutputBucket]
    public function testDirectGrantTableIsRegisteredWhenUploadTablesReturns(): void
    {
        if ($this->clientWrapper->getToken()->getProjectBackend() !== 'snowflake') {
            self::markTestSkipped('The table is written into the bucket schema with Snowflake SQL.');
        }

        $destinationTableId = $this->emptyOutputBucketId . '.table1';
        $this->initDirectGrantWorkspace($destinationTableId);
        $this->createTableThroughDirectGrant($this->emptyOutputBucketId, 'table1');

        $this->uploadDirectGrantTable($destinationTableId)->waitForAll();

        // The refresh job is what registers the table written through the direct grant, so it is in Storage only
        // if uploadTables() really waited for that job.
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables);
        self::assertSame($destinationTableId, $tables[0]['id']);
    }

    private function initDirectGrantWorkspace(string $destinationTableId): void
    {
        $this->initConfigurationWorkspace([
            [
                'source' => 'table1a',
                'destination' => $destinationTableId,
                'unload_strategy' => 'direct-grant',
            ],
        ]);
    }

    private function uploadDirectGrantTable(string $destinationTableId): LoadTableQueue
    {
        $tableLoader = $this->getTableLoader(
            clientWrapper: $this->clientWrapper,
            logger: $this->testLogger,
            strategyFactory: $this->getWorkspaceStagingFactory(
                clientWrapper: $this->clientWrapper,
                logger: $this->testLogger,
            ),
        );

        return $tableLoader->uploadTables(
            new OutputMappingSettings(
                [
                    'mapping' => [
                        [
                            'source' => 'table1a',
                            'destination' => $destinationTableId,
                            'unload_strategy' => 'direct-grant',
                        ],
                    ],
                ],
                '',
                $this->clientWrapper->getToken(),
                false,
                OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
            ),
            new SystemMetadata([
                'componentId' => 'testComponent',
                'configurationId' => 'metadata-write-test',
                'runId' => '1234567',
            ]),
        );
    }

    /**
     * Writes a table straight into the bucket schema, which is what a transformation using direct grants does -
     * the table exists in the backend before output mapping runs and only its Storage metadata is missing.
     */
    private function createTableThroughDirectGrant(string $bucketId, string $tableName): void
    {
        $bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($bucketId);
        $backendPath = $bucket['backendPath'] ?? throw new RuntimeException(sprintf(
            'Bucket "%s" detail carries no backendPath.',
            $bucketId,
        ));
        [$database, $schema] = $backendPath;

        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
        $workspaces->executeQuery((int) $this->workspaceId, sprintf(
            'CREATE TABLE "%s"."%s"."%s" ("id" VARCHAR, "name" VARCHAR)',
            $database,
            $schema,
            $tableName,
        ));
        $workspaces->executeQuery((int) $this->workspaceId, sprintf(
            'INSERT INTO "%s"."%s"."%s" VALUES (\'1\', \'test\')',
            $database,
            $schema,
            $tableName,
        ));
    }
}
