<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\Strategy\Local;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\Test\TestLogger;

class DownloadTablesDefaultTest extends DownloadTablesTestAbstract
{
    public function setUp()
    {
        parent::setUp();
        try {
            $this->clientWrapper->getBasicClient()->dropBucket("in.c-input-mapping-test-default", ["force" => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket(
            "input-mapping-test-default",
            Client::STAGE_IN,
            "Input Mapping Testsuite"
        );

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name", "foo", "bar"]);
        $csv->writeRow(["id1", "name1", "foo1", "bar1"]);
        $csv->writeRow(["id2", "name2", "foo2", "bar2"]);
        $csv->writeRow(["id3", "name3", "foo3", "bar3"]);
        $this->clientWrapper->getBasicClient()->createTableAsync("in.c-input-mapping-test-default", "test", $csv);
        $this->clientWrapper->getBasicClient()->createTableAsync("in.c-input-mapping-test-default", "test2", $csv);
    }

    public function testReadTablesDefaultBackend()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger));
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-input-mapping-test-default.test",
                "destination" => "test.csv"
            ],
            [
                "source" => "in.c-input-mapping-test-default.test2",
                "destination" => "test2.csv"
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        $expectedCSVContent =  "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n";

        self::assertCSVEquals(
            $expectedCSVContent,
            $this->temp->getTmpFolder() . "/download/test.csv"
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-input-mapping-test-default.test", $manifest["id"]);

        self::assertCSVEquals(
            $expectedCSVContent,
            $this->temp->getTmpFolder() . "/download/test2.csv"
        );
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test2.csv.manifest");
        self::assertEquals("in.c-input-mapping-test-default.test2", $manifest["id"]);
        self::assertTrue($logger->hasInfoThatContains('Processing 2 local table exports.'));
    }

    public function testReadTablesEmptyDaysFilter()
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-input-mapping-test-default.test",
                "destination" => "test.csv",
                "days" => 0,
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );
    }

    public function testReadTablesEmptyChangedSinceFilter()
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-input-mapping-test-default.test",
                "destination" => "test.csv",
                "changed_since" => "",
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );
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
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $metadata->postTableMetadata('in.c-input-mapping-test-default.test', 'dataLoaderTest', $tableMetadata);
        $metadata->postColumnMetadata('in.c-input-mapping-test-default.test.Name', 'dataLoaderTest', $columnMetadata);
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-input-mapping-test-default.test",
                "destination" => "test.csv",
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-input-mapping-test-default.test", $manifest["id"]);
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
        self::assertCount(4, $manifest['column_metadata']);
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

    public function testReadTablesWithSourceSearch()
    {
        $tableMetadata = [
            [
                'key' => 'foo',
                'value' => 'bar'
            ]
        ];
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $metadata->postTableMetadata('in.c-input-mapping-test-default.test', 'dataLoaderTest', $tableMetadata);
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                "source_search" => [
                    'key' => 'foo',
                    'value' => 'bar',
                ],
                "destination" => "test.csv",
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-input-mapping-test-default.test", $manifest["id"]);
        self::assertArrayHasKey('metadata', $manifest);
        self::assertCount(1, $manifest['metadata']);
        self::assertArrayHasKey('id', $manifest['metadata'][0]);
        self::assertArrayHasKey('key', $manifest['metadata'][0]);
        self::assertArrayHasKey('value', $manifest['metadata'][0]);
        self::assertArrayHasKey('provider', $manifest['metadata'][0]);
        self::assertArrayHasKey('timestamp', $manifest['metadata'][0]);
        self::assertEquals('dataLoaderTest', $manifest['metadata'][0]['provider']);
        self::assertEquals('foo', $manifest['metadata'][0]['key']);
        self::assertEquals('bar', $manifest['metadata'][0]['value']);
    }

    public function testReadTableColumns()
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
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $metadata->postTableMetadata('in.c-input-mapping-test-default.test', 'dataLoaderTest', $tableMetadata);
        $metadata->postColumnMetadata(
            'in.c-input-mapping-test-default.test.Name',
            'dataLoaderTest',
            [
                [
                    'key' => 'someKey',
                    'value' => 'someValue'
                ]
            ]
        );
        $metadata->postColumnMetadata(
            'in.c-input-mapping-test-default.test.bar',
            'dataLoaderTest',
            [
                [
                    'key' => 'someBarKey',
                    'value' => 'someBarValue'
                ]
            ]
        );
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-input-mapping-test-default.test",
                "columns" => ["bar", "foo", "Id"],
                "destination" => "test.csv",
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        self::assertCSVEquals(
            "\"bar\",\"foo\",\"Id\"\n\"bar1\",\"foo1\",\"id1\"" .
            "\n\"bar2\",\"foo2\",\"id2\"\n\"bar3\",\"foo3\",\"id3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-input-mapping-test-default.test", $manifest["id"]);
        self::assertArrayHasKey('columns', $manifest);
        self::assertEquals(['bar', 'foo', "Id"], $manifest['columns']);
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
        self::assertCount(3, $manifest['column_metadata']);
        self::assertArrayHasKey('Id', $manifest['column_metadata']);
        self::assertArrayHasKey('foo', $manifest['column_metadata']);
        self::assertArrayHasKey('bar', $manifest['column_metadata']);
        self::assertCount(0, $manifest['column_metadata']['Id']);
        self::assertCount(0, $manifest['column_metadata']['foo']);
        self::assertCount(1, $manifest['column_metadata']['bar']);
        self::assertArrayHasKey('id', $manifest['column_metadata']['bar'][0]);
        self::assertArrayHasKey('key', $manifest['column_metadata']['bar'][0]);
        self::assertArrayHasKey('value', $manifest['column_metadata']['bar'][0]);
        self::assertArrayHasKey('provider', $manifest['column_metadata']['bar'][0]);
        self::assertArrayHasKey('timestamp', $manifest['column_metadata']['bar'][0]);
        self::assertEquals('someBarKey', $manifest['column_metadata']['bar'][0]['key']);
        self::assertEquals('someBarValue', $manifest['column_metadata']['bar'][0]['value']);
    }

    public function testReadTableColumnsDataTypes()
    {
        $reader = new Reader($this->getStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-input-mapping-test-default.test",
                "column_types" => [
                     [
                         "source" => "bar",
                         "type" => "NUMERIC",
                     ],
                     [
                         "source" => "foo",
                         "type" => "VARCHAR",
                     ],
                ],
                "destination" => "test.csv",
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );

        self::assertCSVEquals(
            "\"bar\",\"foo\"\n\"bar1\",\"foo1\"" .
            "\n\"bar2\",\"foo2\"\n\"bar3\",\"foo3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "download/test.csv"
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test.csv.manifest");
        self::assertEquals("in.c-input-mapping-test-default.test", $manifest["id"]);
        self::assertArrayHasKey('columns', $manifest);
        self::assertEquals(['bar', 'foo'], $manifest['columns']);
        self::assertArrayHasKey('metadata', $manifest);
        self::assertCount(0, $manifest['metadata']);
        self::assertCount(2, $manifest['column_metadata']);
        self::assertCount(0, $manifest['column_metadata']['bar']);
        self::assertCount(0, $manifest['column_metadata']['foo']);
    }

    public function testReadTableLimitTest()
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $tokenInfo['owner']['limits'][Local::EXPORT_SIZE_LIMIT_NAME] = [
            'name' => Local::EXPORT_SIZE_LIMIT_NAME,
            'value' => 10,
        ];
        $client = self::getMockBuilder(Client::class)
            ->setMethods(['verifyToken'])
            ->setConstructorArgs([['token' => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]])
            ->getMock();
        $client->method('verifyToken')->willReturn($tokenInfo);
        /** @var Client $client */
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);

        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-input-mapping-test-default.test",
                "destination" => "test.csv"
            ]
        ]);

        self::expectException(InvalidInputException::class);
        self::expectExceptionMessageRegExp(
            '#Table "in\.c-input-mapping-test-default\.test" with size [0-9]+ bytes exceeds the input mapping limit ' .
            'of 10 bytes\. Please contact support to raise this limit$#'
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
    }

    public function testReadTablesDevBucket()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->getStagingFactory(null, 'json', $logger));
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-input-mapping-test-default.test",
                "destination" => "test.csv"
            ],
            [
                "source" => "in.c-input-mapping-test-default.test2",
                "destination" => "test2.csv"
            ]
        ]);
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $metadata->postBucketMetadata(
            'in.c-input-mapping-test-default',
            'test',
            [
                [
                    'key' => 'KBC.lastUpdatedBy.branch.id',
                    'value' => '1234',
                ],
            ]
        );

        // without the check it passes
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(false)
        );

        // with the check it fails
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage(
            'The buckets "in.c-input-mapping-test-default" come from a development ' .
            'branch and must not be used directly in input mapping.'
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            StrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
    }
}
