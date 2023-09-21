<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\Result\Column;
use Keboola\InputMapping\Table\Result\MetadataItem;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;

class DownloadTablesOutputTest extends AbstractTestCase
{
    #[NeedsTestTables(2)]
    public function testDownloadTablesResult(): void
    {
        $metadataApi = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $metadataApi->postTableMetadataWithColumns(
            new TableMetadataUpdateOptions(
                $this->firstTableId,
                'someProvider',
                [[
                    'key' => 'foo',
                    'value' => 'bar',
                ]],
                [
                    'Id' => [[
                        'key' => 'someKey',
                        'value' => 'someValue',
                    ]],
                ],
            ),
        );

        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
            ],
            [
                'source' => $this->secondTableId,
                'destination' => 'test2.csv',
            ],
        ]);

        $tablesResult = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
        $test1TableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->firstTableId);
        $test2TableInfo = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->secondTableId);
        self::assertEquals(
            $test1TableInfo['lastImportDate'],
            $tablesResult->getInputTableStateList()->getTable($this->firstTableId)->getLastImportDate(),
        );
        self::assertEquals(
            $test2TableInfo['lastImportDate'],
            $tablesResult->getInputTableStateList()->getTable($this->secondTableId)->getLastImportDate(),
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv',
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test2.csv',
        );
        self::assertCount(2, $tablesResult->getInputTableStateList()->jsonSerialize());
        $tableMetrics = $tablesResult->getMetrics()?->getTableMetrics();
        self::assertNotNull($tableMetrics);
        $metrics = iterator_to_array($tableMetrics);
        self::assertEquals($this->firstTableId, $metrics[0]->getTableId());
        self::assertEquals($this->secondTableId, $metrics[1]->getTableId());
        self::assertSame(0, $metrics[0]->getUncompressedBytes());
        self::assertGreaterThan(0, $metrics[0]->getCompressedBytes());
        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tablesResult->getTables());
        self::assertSame($this->firstTableId, $tables[0]->getId());
        self::assertSame($this->secondTableId, $tables[1]->getId());
        self::assertSame('test1', $tables[0]->getName());
        self::assertSame('test1', $tables[0]->getDisplayName());
        self::assertNull($tables[0]->getSourceTableId());
        self::assertSame($test1TableInfo['lastImportDate'], $tables[0]->getLastImportDate());
        self::assertSame($test1TableInfo['lastChangeDate'], $tables[0]->getLastChangeDate());
        /** @var Column[] $columns */
        $columns = iterator_to_array($tables[0]->getColumns());
        self::assertSame('Id', $columns[0]->getName());
        /** @var MetadataItem[] $metadata */
        $metadata = iterator_to_array($columns[0]->getMetadata());
        self::assertSame('someKey', $metadata[0]->getKey());
        self::assertSame('someValue', $metadata[0]->getValue());
        self::assertSame('someProvider', $metadata[0]->getProvider());
        self::assertNotEmpty($metadata[0]->getTimestamp());
    }
}
