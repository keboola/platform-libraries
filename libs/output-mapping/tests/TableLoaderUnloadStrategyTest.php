<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\Workspaces;

class TableLoaderUnloadStrategyTest extends AbstractTestCase
{
    #[NeedsEmptyOutputBucket]
    #[NeedsTestTables]
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

        $logRecords = $this->testHandler->getRecords();
        $loadingMessages = array_filter(
            $logRecords,
            fn($record) => is_string($record['message']) && str_contains($record['message'], 'Loading table'),
        );
        self::assertCount(1, $loadingMessages, 'Should log loading message for the table');
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
    #[NeedsTestTables]
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
        $result->waitForAll();

        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($this->emptyOutputBucketId);
        self::assertCount(1, $tables, 'Table should be imported when unload_strategy is not set');
        self::assertEquals($this->emptyOutputBucketId . '.table1', $tables[0]['id']);

        $logRecords = $this->testHandler->getRecords();
        $unloadWarnings = array_filter(
            $logRecords,
            fn($record) => is_string($record['message']) && str_contains($record['message'], 'Workspace unload failed'),
        );
        self::assertCount(0, $unloadWarnings, 'Should not attempt workspace unload when no direct-grant mappings');
    }

    #[NeedsEmptyOutputBucket]
    #[NeedsTestTables]
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
}
