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
use Keboola\InputMapping\Tests\Needs\NeedsTestRedshiftTable;
use Keboola\StorageApi\Options\FileUploadOptions;

class DownloadTablesRedshiftTest extends AbstractTestCase
{
    #[NeedsTestRedshiftTable]
    public function testReadTablesRedshift(): void
    {
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->redshiftTableId,
                'destination' => 'test-redshift.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        self::assertEquals(
            // phpcs:ignore Generic.Files.LineLength
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            file_get_contents($this->temp->getTmpFolder(). '/download/test-redshift.csv')
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test-redshift.csv.manifest');
        self::assertEquals($this->redshiftTableId, $manifest['id']);
    }

    #[NeedsTestRedshiftTable]
    public function testReadTablesS3Redshift(): void
    {
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->redshiftTableId,
                'destination' => 'test-redshift.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::S3,
            new ReaderOptions(true)
        );
        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test-redshift.csv.manifest');
        self::assertEquals($this->redshiftTableId, $manifest['id']);
        $this->assertS3info($manifest);
    }

    #[NeedsTestRedshiftTable]
    public function testReadTablesEmptySlices(): void
    {
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('emptyfile');
        $uploadFileId = $this->clientWrapper->getBasicClient()->uploadSlicedFile([], $fileUploadOptions);
        $columns = ['Id', 'Name'];
        $headerCsvFile = new CsvFile($this->temp->getTmpFolder() . 'header.csv');
        $headerCsvFile->writeRow($columns);
        $this->clientWrapper->getBasicClient()->createTableAsync(
            $this->redshiftBucketId,
            'empty',
            $headerCsvFile,
            []
        );

        $options['columns'] = $columns;
        $options['dataFileId'] = $uploadFileId;
        $this->clientWrapper->getBasicClient()->writeTableAsyncDirect($this->redshiftBucketId . '.empty', $options);

        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->redshiftBucketId . '.empty',
                'destination' => 'empty.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
        $file = file_get_contents($this->temp->getTmpFolder() . '/download/empty.csv');
        self::assertEquals("\"Id\",\"Name\"\n", $file);

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/empty.csv.manifest');
        self::assertEquals($this->redshiftBucketId . '.empty', $manifest['id']);
    }
}
