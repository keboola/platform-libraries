<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\Result\Column;
use Keboola\InputMapping\Table\Result\MetadataItem;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\InputMapping\Table\Result\TableMetrics;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;

class DownloadTablesOutputTest extends DownloadTablesTestAbstract
{
    public function setUp(): void
    {
        parent::setUp();
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('in.c-input-mapping-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket('input-mapping-test', Client::STAGE_IN);

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-input-mapping-test', 'test', $csv);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-input-mapping-test', 'test2', $csv);
        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $metadataApi->postColumnMetadata(
            'in.c-input-mapping-test.test.Id',
            'someProvider',
            [[
                'key' => 'someKey',
                'value' => 'someValue',
            ]]
        );
    }

    public function testDownloadTablesResult(): void
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test',
                'destination' => 'test.csv',
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2.csv',
            ],
        ]);

        $tablesResult = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
        $test1TableInfo = $this->clientWrapper->getBasicClient()->getTable('in.c-input-mapping-test.test');
        $test2TableInfo = $this->clientWrapper->getBasicClient()->getTable('in.c-input-mapping-test.test2');
        self::assertEquals(
            $test1TableInfo['lastImportDate'],
            $tablesResult->getInputTableStateList()->getTable('in.c-input-mapping-test.test')->getLastImportDate()
        );
        self::assertEquals(
            $test2TableInfo['lastImportDate'],
            $tablesResult->getInputTableStateList()->getTable('in.c-input-mapping-test.test2')->getLastImportDate()
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv'
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test2.csv'
        );
        self::assertCount(2, $tablesResult->getInputTableStateList()->jsonSerialize());
        $tableMetrics = $tablesResult->getMetrics()?->getTableMetrics();
        self::assertNotNull($tableMetrics);
        $metrics = iterator_to_array($tableMetrics);
        self::assertEquals('in.c-input-mapping-test.test', $metrics[0]->getTableId());
        self::assertEquals('in.c-input-mapping-test.test2', $metrics[1]->getTableId());
        self::assertSame(0, $metrics[0]->getUncompressedBytes());
        self::assertGreaterThan(0, $metrics[0]->getCompressedBytes());
        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tablesResult->getTables());
        self::assertSame('in.c-input-mapping-test.test', $tables[0]->getId());
        self::assertSame('in.c-input-mapping-test.test2', $tables[1]->getId());
        self::assertSame('test', $tables[0]->getName());
        self::assertSame('test', $tables[0]->getDisplayName());
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
