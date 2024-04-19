<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Storage\StoragePreparer;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsRemoveBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;

class StoragePreparerTest extends AbstractTestCase
{
    #[NeedsRemoveBucket('in.c-main')]
    public function testPrepareBucket(): void
    {
        $storagePreparer = new StoragePreparer($this->clientWrapper, $this->testLogger);

        self::assertFalse($this->clientWrapper->getTableAndFileStorageClient()->bucketExists('in.c-main'));

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration(),
            $this->createSystemMetadata(),
        );

        self::assertTrue($this->clientWrapper->getTableAndFileStorageClient()->bucketExists('in.c-main'));
    }

    #[NeedsTestTables(1)]
    public function testPrepareStorageWithExistingTable(): void
    {
        $storagePreparer = new StoragePreparer($this->clientWrapper, $this->testLogger);

        self::assertTrue(
            $this->clientWrapper
                ->getTableAndFileStorageClient()
                ->bucketExists('in.c-testPrepareStorageWithExistingTableTest'),
        );
        $table = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable('in.c-testPrepareStorageWithExistingTableTest.test1');

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => 'in.c-testPrepareStorageWithExistingTableTest.test1',
            ]),
            $this->createSystemMetadata(),
        );

        $updatedTable = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable('in.c-testPrepareStorageWithExistingTableTest.test1');

        self::assertEquals($table, $updatedTable);
    }

    #[NeedsTestTables(1)]
    public function testPrepareStorageWithChangeTableStructure(): void
    {
        $storagePreparer = new StoragePreparer($this->clientWrapper, $this->testLogger);

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => 'in.c-testPrepareStorageWithChangeTableStructureTest.test1',
            ]),
            $this->createSystemMetadata(),
        );

        $table = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable('in.c-testPrepareStorageWithChangeTableStructureTest.test1');

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => 'in.c-testPrepareStorageWithChangeTableStructureTest.test1',
                'columns' => array_merge($table['columns'], [
                    'newColumn',
                ]),
                'primary_key' => array_merge($table['primaryKey'], [
                    'Id',
                ]),
            ]),
            $this->createSystemMetadata(),
        );

        $updatedTable = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable('in.c-testPrepareStorageWithChangeTableStructureTest.test1');

        $expectedTable = array_merge_recursive($table, [
            'columns' => ['newColumn'],
            'primaryKey' => ['Id'],
        ]);

        self::assertEquals($this->dropTimestampParams($expectedTable), $this->dropTimestampParams($updatedTable));
    }

    #[NeedsTestTables(1)]
    public function testPrepareStorageWithChangeTableData(): void
    {
        $storagePreparer = new StoragePreparer($this->clientWrapper, $this->testLogger);

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => 'in.c-testPrepareStorageWithChangeTableDataTest.test1',
            ]),
            $this->createSystemMetadata(),
        );

        $table = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable('in.c-testPrepareStorageWithChangeTableDataTest.test1');

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration([
                'destination' => 'in.c-testPrepareStorageWithChangeTableDataTest.test1',
                'delete_where_column' => 'Id',
                'delete_where_operator' => 'eq',
                'delete_where_values' => ['id1', 'id2'],
            ]),
            $this->createSystemMetadata(),
        );

        $updatedTable = $this->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable('in.c-testPrepareStorageWithChangeTableDataTest.test1');

        $expectedTable = $table;
        $expectedTable['rowsCount'] -= 2;
        $expectedTable['bucket']['rowsCount'] -= 2;

        self::assertEquals($this->dropTimestampParams($expectedTable), $this->dropTimestampParams($updatedTable));
    }

    private function createMappingFromProcessedConfiguration(array $newMapping = []): MappingFromProcessedConfiguration
    {
        $mapping = array_merge([
            'destination' => 'in.c-main.table',
            'delimiter' => ',',
            'enclosure' => '"',
        ], $newMapping);

        $source = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);

        return new MappingFromProcessedConfiguration(
            $mapping,
            $source,
        );
    }

    private function createSystemMetadata(): SystemMetadata
    {
        return new SystemMetadata([
            'componentId' => 'keboola.output-mapping',
        ]);
    }

    private function dropTimestampParams(array $table): array
    {
        unset($table['lastChangeDate']);
        unset($table['bucket']['lastChangeDate']);
        return $table;
    }
}
