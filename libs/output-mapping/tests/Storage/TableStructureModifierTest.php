<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

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

    private TableInfo $destinationTableInfo;

    private MappingDestination $destination;

    public function setUp(): void
    {
        parent::setUp();
        $this->modifier = new TableStructureModifier($this->clientWrapper, $this->testLogger);
        $bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->testBucketId);
        $this->destinationBucket = new BucketInfo($bucket);
        $this->destinationTableInfo = new TableInfo(
            $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId),
        );
        $this->destination = new MappingDestination($this->firstTableId);
    }

    #[NeedsTestTables]
    public function testUpdateTableStructure(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo->getPrimaryKey());
        $source->method('getColumns')->willReturn($this->destinationTableInfo->getColumns());
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            $this->destinationTableInfo,
            $source,
            $this->destination,
        );

        $this->assertTrue(true);
    }

    #[NeedsTestTables]
    public function testUpdateTableStructureAddColumns(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo->getPrimaryKey());
        $source->method('getColumns')->willReturn(array_merge(
            $this->destinationTableInfo->getColumns(),
            ['new_column'],
        ));
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            $this->destinationTableInfo,
            $source,
            $this->destination,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertCount(count($this->destinationTableInfo->getColumns()) + 1, $newTable['columns']);
        $this->assertTrue(in_array('new_column', ($newTable['columns'])));
    }

    #[NeedsTestTables]
    public function testUpdateTableStructureAddPK(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn(array_merge(
            $this->destinationTableInfo->getPrimaryKey(),
            ['Id', 'Name'],
        ));
        $source->method('getColumns')->willReturn($this->destinationTableInfo->getColumns());
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            $this->destinationTableInfo,
            $source,
            $this->destination,
        );

        $newTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);

        $this->assertCount(count($this->destinationTableInfo->getPrimaryKey()) + 2, $newTable['primaryKey']);
        $this->assertEquals(['Id', 'Name'], $newTable['primaryKey']);
    }

    #[NeedsTestTables]
    public function testErrorUpdateTableStructureBadPK(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn(array_merge(
            $this->destinationTableInfo->getPrimaryKey(),
            ['invalidPK'],
        ));
        $source->method('getColumns')->willReturn($this->destinationTableInfo->getColumns());
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            $this->destinationTableInfo,
            $source,
            $this->destination,
        );

        $expectedErrorMessage = 'Error changing primary key of table in.c-testUpdateTableStructureBadPKTest.test1:';
        $expectedErrorMessage .= ' Primary key columns "invalidPK" not found in "Id, Name, foo, bar"';
        $this->assertTrue($this->testHandler->hasWarning($expectedErrorMessage));
    }

    #[NeedsTestTables]
    public function testErrorUpdateTableStructureEmptyColumnName(): void
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);
        $source->method('getPrimaryKey')->willReturn($this->destinationTableInfo->getPrimaryKey());
        $source->method('getColumns')->willReturn(array_merge(
            $this->destinationTableInfo->getColumns(),
            [''],
        ));
        $source->method('getMetadata')->willReturn([]);
        $source->method('getColumnMetadata')->willReturn([]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid parameters - name: This value should not be blank.');
        $this->modifier->updateTableStructure(
            $this->destinationBucket,
            $this->destinationTableInfo,
            $source,
            $this->destination,
        );
    }
}
