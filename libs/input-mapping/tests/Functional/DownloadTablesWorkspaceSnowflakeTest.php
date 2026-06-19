<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;

class DownloadTablesWorkspaceSnowflakeTest extends AbstractTestCase
{
    #[NeedsTestTables(3), NeedsEmptyOutputBucket]
    public function testTablesSnowflakeBackend(): void
    {
        $clientWrapper = $this->initClient();
        $runId = $clientWrapper->getBasicClient()->generateRunId();
        $clientWrapper->getBranchClient()->setRunId($runId);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getWorkspaceStagingFactory(
                clientWrapper: $clientWrapper,
                logger: $this->testLogger,
            ),
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
            ],
            [
                'source' => $this->secondTableId,
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
            ],
            [
                'source' => $this->thirdTableId,
                'destination' => 'test3',
                'keep_internal_timestamp_column' => false,
            ],
        ]);

        $result = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            new ReaderOptions(true),
        );

        // there is now a single unified workspaceLoad job, so should have 1 metrics entry
        $metrics = $result->getMetrics()?->getTableMetrics();
        self::assertNotNull($metrics);
        self::assertCount(1, $metrics);

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->firstTableId,
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1'],
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('Invalid columns: _timestamp:', $e->getMessage());
        }

        // this is copy, so it doesn't contain the _timestamp column
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->secondTableId, $manifest['id']);
        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2'],
        );
        self::assertTrue($clientWrapper->getTableAndFileStorageClient()->tableExists(
            $this->emptyOutputBucketId . '.test2',
        ));

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test3.manifest');
        self::assertEquals($this->thirdTableId, $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it. This time it
            doesn't fail because keep_internal_timestamp_column=false was provided */
        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test3', 'name' => 'test3'],
        );
        self::assertTrue($clientWrapper->getTableAndFileStorageClient()->tableExists(
            $this->emptyOutputBucketId . '.test3',
        ));
        self::assertTrue($this->testHandler->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Loading 3 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
        // test that all inputs are merged into a single workspaceLoad job
        sleep(2);
        $jobs = $clientWrapper->getTableAndFileStorageClient()->listJobs(['limit' => 200]);
        $params = null;
        foreach ($jobs as $job) {
            if ($runId !== $job['runId']) {
                continue;
            }
            if ($job['operationName'] === 'workspaceLoad') {
                $params = $job['operationParams'];
                break;
            }
        }
        self::assertNotEmpty($params);
        // all 3 inputs in a single job, in input-mapping order; the endpoint resolves the load type
        // per item and echoes it back in operationParams
        self::assertEquals(3, count($params['input']));
        self::assertEquals('test1', $params['input'][0]['destination']);
        self::assertEquals('CLONE', $params['input'][0]['loadType']);
        self::assertEquals('test2', $params['input'][1]['destination']);
        self::assertEquals('COPY', $params['input'][1]['loadType']);
        self::assertEquals('test3', $params['input'][2]['destination']);
        self::assertEquals('CLONE', $params['input'][2]['loadType']);
    }

    #[NeedsTestTables(2)]
    public function testTablesInvalidMapping(): void
    {
        $clientWrapper = $this->initClient();

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getWorkspaceStagingFactory($clientWrapper),
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
                'days' => 3,
            ],
            [
                'source' => $this->secondTableId,
                'destination' => 'test2',
            ],
        ]);

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('Days option is not supported on workspace, use changed_since instead.');
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            new ReaderOptions(true),
        );
    }

    #[NeedsTestTables, NeedsEmptyOutputBucket]
    public function testTablesSnowflakeDataTypes(): void
    {
        $clientWrapper = $this->initClient();

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getWorkspaceStagingFactory(
                clientWrapper: $clientWrapper,
                logger: $this->testLogger,
            ),
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'column_types' => [
                    [
                        'source' => 'Id',
                        'destination' => 'MyId',
                        'type' => 'VARCHAR',
                        'convert_empty_values_to_null' => true,
                    ],
                ],
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            new ReaderOptions(true),
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        self::assertEquals(
            ['Id'],
            $manifest['columns'],
        );
        // check that the table exists in the workspace
        $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
            $this->emptyOutputBucketId,
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2'],
        );

        self::assertTrue($this->testHandler->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Loading 1 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    #[NeedsTestTables]
    public function testTablesSnowflakeDataTypesInvalid(): void
    {
        $clientWrapper = $this->initClient();

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getWorkspaceStagingFactory($clientWrapper),
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['id2'],
                'column_types' => [
                    [
                        'source' => 'Id',
                        'destination' => 'MyId',
                        'type' => 'NUMERIC',
                    ],
                ],
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Numeric value \'id2\' is not recognized',
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            new ReaderOptions(true),
        );
    }

    #[NeedsTestTables(2), NeedsDevBranch]
    public function testWorkspaceInputMappingRealDevStorage(): void
    {
        $clientWrapper = $this->initClient();
        $bucket = $clientWrapper->getTableAndFileStorageClient()->getBucket($this->testBucketId);
        $bucketName = $bucket['displayName'];

        $clientOptions = $clientWrapper->getClientOptionsReadOnly()
            ->setBranchId($this->devBranchId)
            ->setUseBranchStorage(true) // this is the important part
        ;

        $clientWrapper = new ClientWrapper($clientOptions);

        // create a table in the branch
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);

        $buckets = [
            $this->testBucketId,
            str_replace('in.c-', 'out.c-', $this->testBucketId),
        ];
        foreach ($buckets as $bucket) {
            try {
                // drop buckets in branch, test satisfyer can't touch this #mc-hammer
                $clientWrapper->getBranchClient()->dropBucket($bucket, ['force' => true, 'async' => true]);
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }

        // create input bucket in branch
        $clientWrapper->getBranchClient()->createBucket(
            $bucketName,
            'in',
        );
        $clientWrapper->getBranchClient()->createTableAsync($this->testBucketId, 'test2', $csv);

        // create output bucket in branch
        $this->emptyOutputBucketId = $clientWrapper->getBranchClient()->createBucket(
            $bucketName,
            'out',
        );

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getWorkspaceStagingFactory(
                clientWrapper: $clientWrapper,
                logger: $this->testLogger,
            ),
        );
        $configuration = new InputTableOptionsList([
            [ // cloned table from production
                'source' => $this->firstTableId,
                'destination' => 'first_clone_table',
                'keep_internal_timestamp_column' => false,
            ],
            [ // copied table from production
                'source' => $this->firstTableId,
                'destination' => 'first_copy_table',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
            ],
            [ // cloned table from branch
                'source' => $this->secondTableId,
                'destination' => 'second_clone_table',
                'keep_internal_timestamp_column' => false,
            ],
            [ // copied table from branch
                'source' => $this->secondTableId,
                'destination' => 'second_copy_table',
                'where_column' => 'Id',
                'where_values' => ['id2', 'id3'],
                'columns' => ['Id'],
            ],
        ]);

        $result = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            new ReaderOptions(true),
        );

        // there is now a single unified workspaceLoad job, so should have 1 metrics entry
        $metrics = $result->getMetrics()?->getTableMetrics();
        self::assertNotNull($metrics);
        self::assertCount(1, $metrics);

        $adapter = new Adapter();

        $tables = [
            'first_clone_table' => $this->firstTableId,
            'first_copy_table' => $this->firstTableId,
            'second_clone_table' => $this->secondTableId,
            'second_copy_table' => $this->secondTableId,
        ];

        foreach ($tables as $table_name => $table_id) {
            $manifest = $adapter->readFromFile(
                $this->temp->getTmpFolder() . '/download/' . $table_name . '.manifest',
            );
            self::assertEquals($table_id, $manifest['id']);
            $clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                [
                    'dataWorkspaceId' => $this->workspaceId,
                    'dataTableName' => $table_name,
                    'name' => 'test' . $table_name,
                ],
            );
            self::assertTrue($clientWrapper->getTableAndFileStorageClient()->tableExists(
                $this->emptyOutputBucketId . '.test' . $table_name,
            ));
        }
        self::assertTrue($this->testHandler->hasInfoThatContains('Using "workspace-snowflake" table input staging.'));
        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf(
                'Using fallback to default branch "%s" for input "%s".',
                $clientWrapper->getDefaultBranch()->id,
                $this->testBucketId .'.test1',
            ),
        ));
        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf(
                'Using fallback to default branch "%s" for input "%s".',
                $clientWrapper->getDefaultBranch()->id,
                $this->testBucketId .'.test1',
            ),
        ));
        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf(
                'Using dev input "%s" from branch "%s" instead of default branch "%s".',
                $this->testBucketId .'.test2',
                $this->devBranchId,
                $clientWrapper->getDefaultBranch()->id,
            ),
        ));
        self::assertTrue(
            $this->testHandler->hasInfoThatContains(
                sprintf(
                    'Using dev input "%s" from branch "%s" instead of default branch "%s".',
                    $this->testBucketId .'.test2',
                    $this->devBranchId,
                    $clientWrapper->getDefaultBranch()->id,
                ),
            ),
        );

        self::assertTrue($this->testHandler->hasInfoThatContains('Loading 4 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
    }
}
