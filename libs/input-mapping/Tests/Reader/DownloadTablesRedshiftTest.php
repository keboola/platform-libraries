<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Psr\Log\NullLogger;

class DownloadTablesRedshiftTest extends DownloadTablesTestAbstract
{
    public function setUp()
    {
        parent::setUp();
        try {
            $this->clientWrapper->getBasicClient()->dropBucket("in.c-docker-test-redshift", ["force" => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket(
            "docker-test-redshift",
            Client::STAGE_IN,
            "Docker Testsuite",
            "redshift"
        );

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name"]);
        $csv->writeRow(["test", "test"]);
        $this->clientWrapper->getBasicClient()->createTableAsync("in.c-docker-test-redshift", "test", $csv);
    }

    public function testReadTablesRedshift()
    {
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test-redshift.test",
                "destination" => "test-redshift.csv"
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'local'
        );

        self::assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($this->temp->getTmpFolder(). "/download/test-redshift.csv")
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test-redshift.csv.manifest");
        self::assertEquals("in.c-docker-test-redshift.test", $manifest["id"]);
    }

    public function testReadTablesS3Redshift()
    {
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test-redshift.test",
                "destination" => "test-redshift.csv"
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            's3'
        );
        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test-redshift.csv.manifest");
        self::assertEquals("in.c-docker-test-redshift.test", $manifest["id"]);
        $this->assertS3info($manifest);
    }

    public function testReadTablesEmptySlices()
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
            'in.c-docker-test-redshift',
            'empty',
            $headerCsvFile,
            []
        );

        $options['columns'] = $columns;
        $options['dataFileId'] = $uploadFileId;
        $this->clientWrapper->getBasicClient()->writeTableAsyncDirect('in.c-docker-test-redshift.empty', $options);

        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test-redshift.empty",
                "destination" => "empty.csv",
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            Reader::STAGING_LOCAL
        );
        $file = file_get_contents($this->temp->getTmpFolder() . "/download/empty.csv");
        self::assertEquals("\"Id\",\"Name\"\n", $file);

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/empty.csv.manifest");
        self::assertEquals("in.c-docker-test-redshift.empty", $manifest["id"]);
    }
}
