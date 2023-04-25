<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Workspace;

use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsTestTables;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;

class WriterWorkspaceTest extends AbstractTestCase
{
    use CreateBranchTrait;

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testSnowflakeTableOutputMapping(): void
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory();
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();

        $this->prepareWorkspaceWithTablesClone($this->testBucketId);

        $root = $this->temp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ],
            [
                'source' => 'table2a',
                'destination' => $this->emptyOutputBucketId . '.table2a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        $this->assertJobParamsMatches([
            'incremental' => true,
            'columns' => ['Id'],
        ], $jobIds[0]);

        $this->assertJobParamsMatches([
            'incremental' => false,
            'columns' => ['Id', 'Name'],
        ], $jobIds[1]);

        $this->assertTablesExists(
            $this->emptyOutputBucketId,
            [
                $this->emptyOutputBucketId . '.table1a',
                $this->emptyOutputBucketId . '.table2a',
            ]
        );
        $this->assertTableRowsEquals(
            $this->emptyOutputBucketId . '.table1a',
            [
                '"id","name","foo","bar"',
                '"id1","name1","foo1","bar1"',
                '"id2","name2","foo2","bar2"',
                '"id3","name3","foo3","bar3"',
            ]
        );
    }

    #[NeedsEmptyOutputBucket]
    public function testTableOutputMappingMissing(): void
    {
        $root = $this->temp->getTmpFolder();
        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        $writer = new TableWriter($this->getWorkspaceStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Failed to load table "' . $this->emptyOutputBucketId .
            '.table1a": Table "table1a" not found in schema "WORKSPACE_'
        );

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
        $tableQueue->waitForAll();
    }

    #[NeedsEmptyOutputBucket]
    public function testTableOutputMappingMissingManifest(): void
    {
        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
            ],
        ];
        $writer = new TableWriter($this->getWorkspaceStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessageMatches('/Table "table1a" not found in schema "WORKSPACE_\d+"$/');

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
        $tableQueue->waitForAll();
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testMappingMerge(): void
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();

        $root = $this->temp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($this->testBucketId);

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
                'metadata' => [
                    [
                        'key' => 'foo',
                        'value' => 'bar',
                    ],
                ],
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                [
                    'columns' => ['Id', 'Name'],
                    'metadata' => [
                        [
                            'key' => 'foo',
                            'value' => 'baz',
                        ],
                        [
                            'key' => 'bar',
                            'value' => 'baz',
                        ],
                    ],
                ]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $tableMetadata = $metadata->listTableMetadata($this->emptyOutputBucketId . '.table1a');
        $tableMetadataValues = [];
        self::assertCount(4, $tableMetadata);
        foreach ($tableMetadata as $item) {
            $tableMetadataValues[$item['key']] = $item['value'];
        }
        self::assertEquals(
            [
                'foo' => 'bar',
                'bar' => 'baz',
                'KBC.createdBy.component.id' => 'foo',
                'KBC.lastUpdatedBy.component.id' => 'foo',
            ],
            $tableMetadataValues
        );
    }

    public function testTableOutputMappingMissingDestinationManifest(): void
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();
        $root = $this->temp->getTmpFolder();
        $configs = [
            [
                'source' => 'table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ],
        ];
        $writer = new TableWriter($factory);
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to resolve destination for output table "table1a".');

        $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
    }

    public function testTableOutputMappingMissingDestinationNoManifest(): void
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();
        $configs = [
            [
                'source' => 'table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ],
        ];
        $writer = new TableWriter($factory);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to resolve destination for output table "table1a".');

        $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testSnowflakeTableOutputBucketNoDestination(): void
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();
        $root = $this->temp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($this->testBucketId);

        $configs = [
            [
                'source' => 'table1a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs, 'bucket' => $this->emptyOutputBucketId],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $this->assertJobParamsMatches([
            'columns' => ['Id', 'Name'],
        ], $jobIds[0]);

        $this->assertTableRowsEquals($this->emptyOutputBucketId . '.table1a', [
            '"id","name","foo","bar"',
            '"id1","name1","foo1","bar1"',
            '"id2","name2","foo2","bar2"',
            '"id3","name3","foo3","bar3"',
        ]);
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testWriteTableOutputMappingDevMode(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER')
            )
        );
        $branchId = $this->createBranch($clientWrapper, self::class);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
                $branchId,
            )
        );

        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();

        $this->prepareWorkspaceWithTables($this->testBucketId);
        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ],
            [
                'source' => 'table2a',
                'destination' => $this->emptyOutputBucketId . '.table2a',
            ],
        ];
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            'workspace-snowflake',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        $jobDetail = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $tableIds[] = $jobDetail['tableId'];
        $jobDetail = $this->clientWrapper->getBasicClient()->getJob($jobIds[1]);
        $tableIds[] = $jobDetail['tableId'];

        self::assertMatchesRegularExpression(
            '#out\.(c-)?' . $branchId . '-testWriteTableOutputMappingDevModeEmpty\.table1a#',
            $tableIds[0]
        );
        self::assertMatchesRegularExpression(
            '#out\.(c-)?' . $branchId . '-testWriteTableOutputMappingDevModeEmpty\.table2a#',
            $tableIds[1]
        );
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testSnowflakeMultipleMappingOfSameSource(): void
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();
        $root = $this->temp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($this->testBucketId);

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
            ],
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a_2',
            ],
        ];
        file_put_contents($root . '/table1a.manifest', json_encode(['columns' => ['Id', 'Name']]));

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        $this->assertTablesExists(
            $this->emptyOutputBucketId,
            [
                $this->emptyOutputBucketId . '.table1a',
                $this->emptyOutputBucketId . '.table1a_2',
            ]
        );
        $this->assertTableRowsEquals($this->emptyOutputBucketId . '.table1a', [
            '"id","name","foo","bar"',
            '"id1","name1","foo1","bar1"',
            '"id2","name2","foo2","bar2"',
            '"id3","name3","foo3","bar3"',
        ]);
        $this->assertTableRowsEquals($this->emptyOutputBucketId . '.table1a_2', [
            '"id","name","foo","bar"',
            '"id1","name1","foo1","bar1"',
            '"id2","name2","foo2","bar2"',
            '"id3","name3","foo3","bar3"',
        ]);
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testWriteOnlyOnJobFailure(): void
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory(
            null,
            'json',
            null,
            [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]
        );
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();
        $root = $this->temp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($this->testBucketId);

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
                'write_always' => false,
            ],
            [
                'source' => 'table2a',
                'destination' => $this->emptyOutputBucketId . '.table2a',
                'write_always' => true,
            ],
        ];
        file_put_contents($root . '/table1a.manifest', json_encode(['columns' => ['Id', 'Name']]));
        file_put_contents($root . '/table2a.manifest', json_encode(['columns' => ['Id', 'Name']]));

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            true
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        $this->assertTableRowsEquals($this->emptyOutputBucketId . '.table2a', [
            '"id","name","foo","bar"',
            '"id1","name1","foo1","bar1"',
            '"id2","name2","foo2","bar2"',
            '"id3","name3","foo3","bar3"',
        ]);
    }

    #[NeedsTestTables(2), NeedsEmptyOutputBucket]
    public function testSnowflakeTableOutputMappingSkipsTimestampColumn(): void
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getWorkspaceStagingFactory();
        // initialize the workspace mock
        $factory->getTableOutputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE
        )->getDataStorage()->getWorkspaceId();

        $this->prepareWorkspaceWithTablesClone($this->testBucketId);

        $root = $this->temp->getTmpFolder();

        $configs = [
            [
                'source' => 'table1a',
                'destination' => $this->emptyOutputBucketId . '.table1a',
                'incremental' => true,
                'columns' => ['Id', '_timestamp'],
            ],
            [
                'source' => 'table2a',
                'destination' => $this->emptyOutputBucketId . '.table2a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                [
                    'column_metadata' => [
                        '_timestamp' => [
                            [
                                'key' => Common::KBC_METADATA_KEY_TYPE,
                                'value' => Snowflake::TYPE_TIMESTAMP_NTZ,
                            ],
                            [
                                'key' => Common::KBC_METADATA_KEY_BASETYPE,
                                'value' => Snowflake::TYPE_TIMESTAMP,
                            ],
                        ],
                    ],
                ]
            )
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name', '_TIMESTAMP']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake',
            false,
            false
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(2, $jobIds);
        self::assertNotEmpty($jobIds[0]);
        self::assertNotEmpty($jobIds[1]);

        $this->assertJobParamsMatches([
            'incremental' => true,
            'columns' => ['Id'],
        ], $jobIds[0]);

        $this->assertJobParamsMatches([
            'incremental' => false,
            'columns' => ['Id', 'Name'],
        ], $jobIds[1]);

        $this->assertTablesExists(
            $this->emptyOutputBucketId,
            [
                $this->emptyOutputBucketId . '.table1a',
                $this->emptyOutputBucketId . '.table2a',
            ]
        );
        $this->assertTableRowsEquals(
            $this->emptyOutputBucketId . '.table1a',
            [
                '"id","name","foo","bar"',
                '"id1","name1","foo1","bar1"',
                '"id2","name2","foo2","bar2"',
                '"id3","name3","foo3","bar3"',
            ]
        );
        $this->assertTableRowsEquals(
            $this->emptyOutputBucketId . '.table2a',
            [
                '"id","name","foo","bar"',
                '"id1","name1","foo1","bar1"',
                '"id2","name2","foo2","bar2"',
                '"id3","name3","foo3","bar3"',
            ]
        );
    }
}
