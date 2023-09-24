<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsStorageBackend;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\Options\FileUploadOptions;

#[NeedsStorageBackend('bigquery')]
class DownloadTablesBigQueryTest extends AbstractTestCase
{
    #[NeedsTestTables]
    public function testReadTablesBigQuery(): void
    {
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test-bigquery.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        $expectedData = [
            [
                'Id' => 'id1',
                'Name' => 'name1',
                'foo' => 'foo1',
                'bar' => 'bar1',
            ],
            [
                'Id' => 'id2',
                'Name' => 'name2',
                'foo' => 'foo2',
                'bar' => 'bar2',
            ],
            [
                'Id' => 'id3',
                'Name' => 'name3',
                'foo' => 'foo3',
                'bar' => 'bar3',
            ],
        ];

        $exportedData = [];
        $csv = new CsvFile($this->temp->getTmpFolder(). '/download/test-bigquery.csv');
        $header = $csv->getHeader();
        foreach ($csv as $i => $row) {
            if (!$i) {
                continue;
            }

            self::assertIsArray($row);
            $exportedData[] = array_combine($header, $row);
        }

        self::assertArrayEqualsSorted(
            $expectedData,
            $exportedData,
            'Id',
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test-bigquery.csv.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
    }

    #[NeedsTestTables]
    public function testReadTablesGcsBigQuery(): void
    {
        $this->markTestSkipped('GCS is not yet implemented');
    }

    #[NeedsEmptyInputBucket]
    public function testReadTablesEmptySlices(): void
    {
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('emptyfile');
        $uploadFileId = $this->clientWrapper->getTableAndFileStorageClient()->uploadSlicedFile([], $fileUploadOptions);
        $columns = ['Id', 'Name'];
        $headerCsvFile = new CsvFile($this->temp->getTmpFolder() . 'header.csv');
        $headerCsvFile->writeRow($columns);
        $tableId = $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $this->emptyInputBucketId,
            'empty',
            $headerCsvFile,
            [],
        );

        $options['columns'] = $columns;
        $options['dataFileId'] = $uploadFileId;
        $this->clientWrapper->getTableAndFileStorageClient()->writeTableAsyncDirect(
            $tableId,
            $options,
        );

        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $tableId,
                'destination' => 'empty.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
        $file = file_get_contents($this->temp->getTmpFolder() . '/download/empty.csv');
        self::assertEquals("\"Id\",\"Name\"\n", $file);

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/empty.csv.manifest');
        self::assertEquals($tableId, $manifest['id']);
    }

    private static function assertArrayEqualsSorted(
        array $expected,
        array $actual,
        string $sortKey,
    ): void {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] === $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        usort($expected, $comparsion);
        usort($actual, $comparsion);
        self::assertEquals($expected, $actual);
    }
}
