<?php

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Reader;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Psr\Log\NullLogger;

class ReaderTablesDefaultTest extends ReaderTablesTestAbstract
{
    public function setUp()
    {
        parent::setUp();
        try {
            $this->client->dropBucket("in.c-docker-test", ["force" => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->client->createBucket("docker-test", Client::STAGE_IN, "Docker Testsuite");

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name"]);
        $csv->writeRow(["test", "test"]);
        $this->client->createTableAsync("in.c-docker-test", "test", $csv);
        $this->client->setTableAttribute("in.c-docker-test.test", "attr1", "val1");
    }

    public function testReadTablesDefaultBackend()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv"
            ]
        ];

        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");

        self::assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($this->temp->getTmpFolder() . "/download/test.csv")
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-docker-test.test", $manifest["id"]);
        self::assertEquals("val1", $manifest["attributes"][0]["value"]);
    }

    public function testReadTablesEmptyDaysFilter()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
                "days" => 0
            ]
        ];

        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");
        self::assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv")
        );
    }

    public function testReadTablesEmptyChangedSinceFilter()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
                "changed_since" => ""
            ]
        ];

        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");
        self::assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv")
        );
    }

    public function testReadTablesS3DefaultBackend()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
            ]
        ];

        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download", "s3");

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-docker-test.test", $manifest["id"]);
        self::assertEquals("val1", $manifest["attributes"][0]["value"]);
        $this->assertS3info($manifest);
    }

    public function testReadTablesMetadata()
    {
        $tableMetadata = [
            [
                'key' => 'foo',
                'value' => 'bar'
            ],
            [
                'key' => 'fooBar',
                'value' => 'baz'
            ]
        ];
        $columnMetadata = [
            [
                'key' => 'someKey',
                'value' => 'someValue'
            ]
        ];
        $metadata = new Metadata($this->client);
        $metadata->postTableMetadata('in.c-docker-test.test', 'dataLoaderTest', $tableMetadata);
        $metadata->postColumnMetadata('in.c-docker-test.test.Name', 'dataLoaderTest', $columnMetadata);
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
            ]
        ];

        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download", "s3");

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-docker-test.test", $manifest["id"]);
        self::assertEquals("val1", $manifest["attributes"][0]["value"]);
        $this->assertS3info($manifest);
        self::assertArrayHasKey('metadata', $manifest);
        self::assertCount(2, $manifest['metadata']);
        self::assertArrayHasKey('id', $manifest['metadata'][0]);
        self::assertArrayHasKey('key', $manifest['metadata'][0]);
        self::assertArrayHasKey('value', $manifest['metadata'][0]);
        self::assertArrayHasKey('provider', $manifest['metadata'][0]);
        self::assertArrayHasKey('timestamp', $manifest['metadata'][0]);
        self::assertArrayHasKey('id', $manifest['metadata'][1]);
        self::assertArrayHasKey('key', $manifest['metadata'][1]);
        self::assertArrayHasKey('value', $manifest['metadata'][1]);
        self::assertArrayHasKey('provider', $manifest['metadata'][1]);
        self::assertArrayHasKey('timestamp', $manifest['metadata'][1]);
        self::assertEquals('dataLoaderTest', $manifest['metadata'][0]['provider']);
        self::assertEquals('foo', $manifest['metadata'][0]['key']);
        self::assertEquals('bar', $manifest['metadata'][0]['value']);
        self::assertEquals('fooBar', $manifest['metadata'][1]['key']);
        self::assertEquals('baz', $manifest['metadata'][1]['value']);
        self::assertCount(2, $manifest['column_metadata']);
        self::assertArrayHasKey('Id', $manifest['column_metadata']);
        self::assertArrayHasKey('Name', $manifest['column_metadata']);
        self::assertCount(0, $manifest['column_metadata']['Id']);
        self::assertCount(1, $manifest['column_metadata']['Name']);
        self::assertArrayHasKey('id', $manifest['column_metadata']['Name'][0]);
        self::assertArrayHasKey('key', $manifest['column_metadata']['Name'][0]);
        self::assertArrayHasKey('value', $manifest['column_metadata']['Name'][0]);
        self::assertArrayHasKey('provider', $manifest['column_metadata']['Name'][0]);
        self::assertArrayHasKey('timestamp', $manifest['column_metadata']['Name'][0]);
        self::assertEquals('someKey', $manifest['column_metadata']['Name'][0]['key']);
        self::assertEquals('someValue', $manifest['column_metadata']['Name'][0]['value']);
    }

    public function testChangedSinceAndDaysConfiguration()
    {
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                "source" => "in.c-docker-test.test",
                "destination" => "test.csv",
                "changed_since" => "-1 days",
                "days" => 1
            ]
        ];

        try {
            $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download");
            $this->fail("Exception not caught");
        } catch (InvalidInputException $e) {
            $this->assertEquals("Cannot set both parameters 'days' and 'changed_since'.", $e->getMessage());
        }

    }
}
