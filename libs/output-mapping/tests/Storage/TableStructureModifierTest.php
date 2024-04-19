<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\OutputMapping\Storage\TableStructureModifier;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\ClientException;

class TableStructureModifierTest extends AbstractTestCase
{
    private TableStructureModifier $modifier;

    private BucketInfo $destinationBucket;

    private array $destinationTableInfo;

    private MappingDestination $destination;

    public function setUp(): void
    {
        parent::setUp();
        $this->modifier = new TableStructureModifier($this->clientWrapper, $this->testLogger);
        $bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->testBucketId);
        $this->destinationBucket = new BucketInfo($bucket);
        $this->destinationTableInfo = $this
            ->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->firstTableId);
        $this->destination = new MappingDestination($this->firstTableId);
    }

    #[NeedsTestTables]
    public function testUpdateTableStructureNoChanges(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo['primaryKey']);
        $source->method('getColumns')->willReturn($this->destinationTableInfo['columns']);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertEquals(
            $this->dropTimestampParams($this->destinationTableInfo),
            $this->dropTimestampParams($newTable),
        );
    }

    #[NeedsTestTables]
    public function testUpdateTableStructureAddColumns(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo['primaryKey']);
        $source->method('getColumns')->willReturn(array_merge(
            $this->destinationTableInfo['columns'],
            ['new_column'],
        ));
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        $expectedTable = array_merge_recursive($this->destinationTableInfo, [
            'columns' => ['new_column'],
        ]);

        $this->assertEquals($this->dropTimestampParams($expectedTable), $this->dropTimestampParams($newTable));
    }

    #[NeedsTestTables]
    public function testUpdateTableStructureAddPK(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn(array_merge(
            $this->destinationTableInfo['primaryKey'],
            ['Id', 'Name'],
        ));
        $source->method('getColumns')->willReturn($this->destinationTableInfo['columns']);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        $expectedTable = array_merge_recursive($this->destinationTableInfo, [
            'primaryKey' => ['Id', 'Name'],
        ]);

        $this->assertEquals($this->dropTimestampParams($expectedTable), $this->dropTimestampParams($newTable));
    }

    #[NeedsTestTables]
    public function testErrorUpdateTableStructureBadPK(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn(array_merge(
            $this->destinationTableInfo['primaryKey'],
            ['invalidPK'],
        ));
        $source->method('getColumns')->willReturn($this->destinationTableInfo['columns']);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
        );

        $expectedErrorMessage = 'Error changing primary key of table in.c-testErrorUpdateTableStructureBadPKTest.test1';
        $expectedErrorMessage .= ': Primary key columns "invalidPK" not found in "Id, Name, foo, bar"';
        $this->assertTrue($this->testHandler->hasWarning($expectedErrorMessage));
    }

    #[NeedsTestTables]
    public function testErrorUpdateTableStructureAddPKOnWrongData(): void
    {
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id5', 'name5', 'foo5', 'bar5']);
        $csv->writeRow(['id5', 'name5', 'foo5', 'bar5']);

        $this->clientWrapper->getTableAndFileStorageClient()->writeTableAsync(
            $this->firstTableId,
            $csv,
        );

        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn(array_merge(
            $this->destinationTableInfo['primaryKey'],
            ['Id'],
        ));
        $source->method('getColumns')->willReturn($this->destinationTableInfo['columns']);
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
        );

        $this->assertTrue($this->testHandler->hasWarningThatContains(
            'The new primary key cannot be created; duplicate values in primary key columns exist.',
        ));
    }

    #[NeedsTestTables]
    public function testErrorUpdateTableStructureEmptyColumnName(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo['primaryKey']);
        $source->method('getColumns')->willReturn(array_merge(
            $this->destinationTableInfo['columns'],
            [''],
        ));
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid parameters - name: This value should not be blank.');
        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            new TableInfo($this->destinationTableInfo),
            $source,
            $this->destination,
        );
    }

    private function dropTimestampParams(array $table): array
    {
        unset($table['lastChangeDate']);
        unset($table['bucket']['lastChangeDate']);
        return $table;
    }
}
