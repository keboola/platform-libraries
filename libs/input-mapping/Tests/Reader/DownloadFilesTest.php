<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Reader;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Psr\Log\NullLogger;
use Symfony\Component\Finder\Finder;

class DownloadFilesTest extends DownloadFilesTestAbstract
{
    public function testReadFiles()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));
        $id2 = $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));
        sleep(2);

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [["tags" => ["docker-bundle-test"]]];
        $reader->downloadFiles($configuration, $root . "/download");

        self::assertEquals("test", file_get_contents($root . "/download/" . $id1 . '_upload'));
        self::assertEquals("test", file_get_contents($root . "/download/" . $id2 . '_upload'));

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . "/download/" . $id1 . "_upload.manifest");
        $manifest2 = $adapter->readFromFile($root . "/download/" . $id2 . "_upload.manifest");

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('name', $manifest1);
        self::assertArrayHasKey('created', $manifest1);
        self::assertArrayHasKey('is_public', $manifest1);
        self::assertArrayHasKey('is_encrypted', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertArrayHasKey('max_age_days', $manifest1);
        self::assertArrayHasKey('size_bytes', $manifest1);
        self::assertArrayHasKey('is_sliced', $manifest1);
        self::assertFalse($manifest1['is_sliced']);
        self::assertEquals($id1, $manifest1["id"]);
        self::assertEquals($id2, $manifest2["id"]);
    }

    public function testReadFilesTagsFilterRunId()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->client, new NullLogger());
        $fo = new FileUploadOptions();
        $fo->setTags(["docker-bundle-test"]);

        $this->client->setRunId('xyz');
        $id1 = $this->client->uploadFile($root . "/upload", $fo);
        $id2 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567');
        $id3 = $this->client->uploadFile($root . "/upload", $fo);
        $id4 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567.8901234');
        $id5 = $this->client->uploadFile($root . "/upload", $fo);
        $id6 = $this->client->uploadFile($root . "/upload", $fo);
        sleep(2);

        $configuration = [["tags" => ["docker-bundle-test"], "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, $root . "/download");

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadFilesEsQueryFilterRunId()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->client, new NullLogger());
        $fo = new FileUploadOptions();
        $fo->setTags(["docker-bundle-test"]);

        $this->client->setRunId('xyz');
        $id1 = $this->client->uploadFile($root . "/upload", $fo);
        $id2 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567');
        $id3 = $this->client->uploadFile($root . "/upload", $fo);
        $id4 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567.8901234');
        $id5 = $this->client->uploadFile($root . "/upload", $fo);
        $id6 = $this->client->uploadFile($root . "/upload", $fo);
        sleep(2);

        $configuration = [["query" => "tags: docker-bundle-test", "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, $root . "/download");

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadFilesLimit()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        // make at least 100 files in the project
        for ($i = 0; $i < 102; $i++) {
            $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));
        }
        sleep(2);

        // valid configuration, but does nothing
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [];
        $reader->downloadFiles($configuration, $root . "/download");

        // invalid configuration
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [[]];
        try {
            $reader->downloadFiles($configuration, $root . "/download");
            self::fail("Invalid configuration should fail.");
        } catch (InvalidInputException $e) {
        }

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [['query' => 'id:>0 AND (NOT tags:table-export)']];
        $reader->downloadFiles($configuration, $root . "/download");
        $finder = new Finder();
        $finder->files()->in($root . "/download")->notName('*.manifest');
        self::assertEquals(100, $finder->count());

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [['tags' => ['docker-bundle-test'], 'limit' => 102]];
        $reader->downloadFiles($configuration, $root . "/download");
        $finder = new Finder();
        $finder->files()->in($root . "/download")->notName('*.manifest');
        self::assertEquals(102, $finder->count());
    }

    public function testReadSlicedFileSnowflake()
    {
        // Create bucket
        $bucketId = 'in.c-docker-test-snowflake';
        if (!$this->client->bucketExists($bucketId)) {
            $this->client->createBucket('docker-test-snowflake', Client::STAGE_IN, "Docker Testsuite");
        }

        // Create redshift table and export it to produce a sliced file
        $tableName = 'test_file';
        $tableId = sprintf('%s.%s', $bucketId, $tableName);
        if (!$this->client->tableExists($tableId)) {
            $csv = new CsvFile($this->tmpDir . "/upload.csv");
            $csv->writeRow(["Id", "Name"]);
            $csv->writeRow(["test", "test"]);
            $this->client->createTableAsync($bucketId, $tableName, $csv);
        }
        $table = $this->client->exportTableAsync($tableId);
        $fileId = $table['file']['id'];

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [['query' => 'id: ' . $fileId]];

        $dlDir = $this->tmpDir . "/download";
        $reader->downloadFiles($configuration, $dlDir);
        $fileName = sprintf('%s_%s.csv', $fileId, $tableId);

        $resultFileContent = '';
        $finder = new Finder();

        /** @var \SplFileInfo $file */
        foreach ($finder->files()->in($dlDir . '/' . $fileName) as $file) {
            $resultFileContent .= file_get_contents($file->getPathname());
        }

        self::assertEquals('"test","test"' . "\n", $resultFileContent);

        $manifestFile = $dlDir . "/" . $fileName . ".manifest";
        self::assertFileExists($manifestFile);
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestFile);
        self::assertArrayHasKey('is_sliced', $manifest);
        self::assertTrue($manifest['is_sliced']);
    }

    public function testReadFilesEmptySlices()
    {
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('empty_file');
        $uploadFileId = $this->client->uploadSlicedFile([], $fileUploadOptions);
        sleep(2);

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                'query' => 'id:' . $uploadFileId,
            ],
        ];
        $reader->downloadFiles($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile(
            $this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file.manifest'
        );
        self::assertEquals($uploadFileId, $manifest['id']);
        self::assertEquals('empty_file', $manifest['name']);
        self::assertDirectoryExists($this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file');
    }
}
