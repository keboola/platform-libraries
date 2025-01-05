<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\Strategy\Local;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\StorageApiBranch\Branch;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class DownloadTablesDefaultTest extends AbstractTestCase
{
    #[NeedsTestTables(2)]
    public function testReadTablesDefaultBackend(): void
    {
        $reader = new Reader($this->getLocalStagingFactory(logger: $this->testLogger));
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

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        $expectedCSVContent =  "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n";

        self::assertCSVEquals(
            $expectedCSVContent,
            $this->temp->getTmpFolder() . '/download/test.csv',
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.csv.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);

        self::assertCSVEquals(
            $expectedCSVContent,
            $this->temp->getTmpFolder() . '/download/test2.csv',
        );
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.csv.manifest');
        self::assertEquals($this->secondTableId, $manifest['id']);
        self::assertTrue($this->testHandler->hasInfoThatContains('Processing 2 local table exports.'));
    }

    #[NeedsTestTables]
    public function testReadTablesEmptyDaysFilter(): void
    {
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
                'days' => 0,
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv',
        );
    }

    #[NeedsTestTables]
    public function testReadTablesEmptyChangedSinceFilter(): void
    {
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
                'changed_since' => '',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv',
        );
    }

    #[NeedsTestTables]
    public function testReadTablesMetadata(): void
    {
        $tableMetadata = [
            [
                'key' => 'foo',
                'value' => 'bar',
            ],
            [
                'key' => 'fooBar',
                'value' => 'baz',
            ],
        ];
        $columnMetadata = [
            'Name' => [
                [
                    'key' => 'someKey',
                    'value' => 'someValue',
                ],
            ],
        ];
        $metadata = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $metadata->postTableMetadataWithColumns(
            new TableMetadataUpdateOptions(
                $this->firstTableId,
                'dataLoaderTest',
                $tableMetadata,
                $columnMetadata,
            ),
        );
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.csv.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
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

    #[NeedsTestTables]
    public function testReadTablesWithSourceSearch(): void
    {
        $bucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->testBucketId);
        $sourceName = $bucket['name'];
        $tableMetadata = [
            [
                'key' => 'source',
                'value' => $sourceName,
            ],
        ];
        $metadata = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $metadata->postTableMetadataWithColumns(
            new TableMetadataUpdateOptions($this->firstTableId, 'dataLoaderTest', $tableMetadata),
        );
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source_search' => [
                    'key' => 'source',
                    'value' => $sourceName,
                ],
                'destination' => 'test.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.csv.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        self::assertArrayHasKey('metadata', $manifest);
        self::assertCount(1, $manifest['metadata']);
        self::assertArrayHasKey('id', $manifest['metadata'][0]);
        self::assertArrayHasKey('key', $manifest['metadata'][0]);
        self::assertArrayHasKey('value', $manifest['metadata'][0]);
        self::assertArrayHasKey('provider', $manifest['metadata'][0]);
        self::assertArrayHasKey('timestamp', $manifest['metadata'][0]);
        self::assertEquals('dataLoaderTest', $manifest['metadata'][0]['provider']);
        self::assertEquals('source', $manifest['metadata'][0]['key']);
        self::assertEquals($sourceName, $manifest['metadata'][0]['value']);
    }

    #[NeedsTestTables]
    public function testReadTableColumns(): void
    {
        $tableMetadata = [
            [
                'key' => 'foo',
                'value' => 'bar',
            ],
            [
                'key' => 'fooBar',
                'value' => 'baz',
            ],
        ];
        $columnMetadata = [
            'Name' => [
                [
                    'key' => 'someKey',
                    'value' => 'someValue',
                ],
            ],
            'bar' => [
                [
                    'key' => 'someBarKey',
                    'value' => 'someBarValue',
                ],
            ],
        ];

        $metadata = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $metadata->postTableMetadataWithColumns(
            new TableMetadataUpdateOptions(
                $this->firstTableId,
                'dataLoaderTest',
                $tableMetadata,
                $columnMetadata,
            ),
        );
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'columns' => ['bar', 'foo', 'Id'],
                'destination' => 'test.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        self::assertCSVEquals(
            "\"bar\",\"foo\",\"Id\"\n\"bar1\",\"foo1\",\"id1\"" .
            "\n\"bar2\",\"foo2\",\"id2\"\n\"bar3\",\"foo3\",\"id3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv',
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.csv.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        self::assertArrayHasKey('columns', $manifest);
        self::assertEquals(['bar', 'foo', 'Id'], $manifest['columns']);
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

    #[NeedsTestTables]
    public function testReadTableColumnsDataTypes(): void
    {
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'column_types' => [
                     [
                         'source' => 'bar',
                         'type' => 'NUMERIC',
                     ],
                     [
                         'source' => 'foo',
                         'type' => 'VARCHAR',
                     ],
                ],
                'destination' => 'test.csv',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );

        self::assertCSVEquals(
            "\"bar\",\"foo\"\n\"bar1\",\"foo1\"" .
            "\n\"bar2\",\"foo2\"\n\"bar3\",\"foo3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv',
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test.csv.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        self::assertArrayHasKey('columns', $manifest);
        self::assertEquals(['bar', 'foo'], $manifest['columns']);
        self::assertArrayHasKey('metadata', $manifest);
        self::assertCount(0, $manifest['metadata']);
        self::assertCount(2, $manifest['column_metadata']);
        self::assertCount(0, $manifest['column_metadata']['bar']);
        self::assertCount(0, $manifest['column_metadata']['foo']);
    }

    #[NeedsTestTables]
    public function testReadTableLimitTest(): void
    {
        $tokenInfo = $this->clientWrapper->getBranchClient()->verifyToken();
        $tokenInfo['owner']['limits'][Local::EXPORT_SIZE_LIMIT_NAME] = [
            'name' => Local::EXPORT_SIZE_LIMIT_NAME,
            'value' => 10,
        ];
        $client = self::getMockBuilder(Client::class)
            ->onlyMethods(['verifyToken'])
            ->setConstructorArgs([
                ['token' => (string) getenv('STORAGE_API_TOKEN'), 'url' => (string) getenv('STORAGE_API_URL')],
            ])
            ->getMock();
        $client->method('verifyToken')->willReturn($tokenInfo);

        $branchClient = self::getMockBuilder(BranchAwareClient::class)
            ->onlyMethods(['verifyToken'])
            ->setConstructorArgs([
                ClientWrapper::BRANCH_DEFAULT,
                ['token' => (string) getenv('STORAGE_API_TOKEN'), 'url' => (string) getenv('STORAGE_API_URL')],
            ])
            ->getMock();
        $branchClient->method('verifyToken')->willReturn($tokenInfo);

        /** @var Client $client */
        $clientWrapper = self::createMock(ClientWrapper::class);
        $clientWrapper->method('getBranchClient')->willReturn($branchClient);
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($client);
        $clientWrapper->method('getDefaultBranch')->willReturn(
            new Branch('123', 'main', true, null),
        );

        $reader = new Reader($this->getLocalStagingFactory($clientWrapper));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
            ],
        ]);

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessageMatches(
            sprintf(
                '#Table "%s" with size [0-9]+ bytes exceeds the input mapping limit ' .
                'of 10 bytes\. Please contact support to raise this limit$#',
                preg_quote($this->firstTableId, '#'),
            ),
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
    }

    #[NeedsTestTables(2)]
    public function testReadTablesDevBucket(): void
    {
        $reader = new Reader($this->getLocalStagingFactory(logger: $this->testLogger));
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
        $metadata = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $metadata->postBucketMetadata(
            $this->testBucketId,
            'test',
            [
                [
                    'key' => 'KBC.lastUpdatedBy.branch.id',
                    'value' => '1234',
                ],
            ],
        );

        // without the check it passes
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(false),
        );

        // with the check it fails
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            sprintf(
                'The buckets "%s" come from a development ' .
                'branch and must not be used directly in input mapping.',
                $this->testBucketId,
            ),
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
    }

    #[NeedsTestTables(1)]
    public function testReadTablesDevBucketProtectedBranch(): void
    {
        $clientWrapper = new ClientWrapper(new ClientOptions(
            url: (string) getenv('STORAGE_API_URL'),
            token: (string) getenv('STORAGE_API_TOKEN'),
            useBranchStorage: true,
        ));
        $reader = new Reader($this->getLocalStagingFactory(clientWrapper: $clientWrapper, logger: $this->testLogger));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
            ],
        ]);
        $metadata = new Metadata($this->clientWrapper->getTableAndFileStorageClient());
        $metadata->postBucketMetadata(
            $this->testBucketId,
            'test',
            [
                [
                    'key' => 'KBC.lastUpdatedBy.branch.id',
                    'value' => '1234',
                ],
            ],
        );

        // without the check it passes
        $result = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(false),
        );

        // with the check it passes too
        $result = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
        self::assertCount(1, $result->getTables());
    }

    #[NeedsTestTables(1)]
    public function testReadTablesDevBranchFallback(): void
    {
        // create a branch
        $masterClientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
            ),
        );
        $branchesApi = new DevBranches($masterClientWrapper->getBasicClient());
        foreach ($branchesApi->listBranches() as $branch) {
            if ($branch['name'] === self::class) {
                $branchesApi->deleteBranch($branch['id']);
            }
        }
        $branchId = (string) $branchesApi->createBranch(self::class)['id'];

        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN'),
                branchId: $branchId,
                useBranchStorage: true,
            ),
        );
        $this->clientWrapper = $clientWrapper;

        $reader = new Reader($this->getLocalStagingFactory($clientWrapper));
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test.csv',
                'changed_since' => '',
                // fails in TableExporter:175 'columns' => ['Id', 'Name'],
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true),
        );
        self::assertCSVEquals(
            "\"Id\",\"Name\",\"foo\",\"bar\"\n\"id1\",\"name1\",\"foo1\",\"bar1\"\n" .
            "\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv',
        );
    }
}
