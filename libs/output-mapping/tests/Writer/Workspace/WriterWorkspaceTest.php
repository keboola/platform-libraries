<?php

namespace Keboola\OutputMapping\Tests\Writer\Workspace;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class WriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    use CreateBranchTrait;

    private const INPUT_BUCKET = 'in.c-WriterWorkspaceTest';
    private const OUTPUT_BUCKET = 'out.c-WriterWorkspaceTest';
    private const FILE_TAG = 'WriterWorkspaceTest';
    private const TEST_BRANCH = 'dev-123';
    private const BRANCH_BUCKET = 'out.c-dev-123-WriterWorkspaceTest';

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([
            self::INPUT_BUCKET,
            self::OUTPUT_BUCKET,
            self::BRANCH_BUCKET,
        ]);
        $this->clearFileUploads([self::FILE_TAG]);
    }

    public function testSnowflakeTableOutputMapping()
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_SNOWFLAKE)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($tokenInfo['owner']['defaultBackend'], 'WriterWorkspaceTest');

        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ],
            [
                'source' => 'table2a',
                'destination' => self::OUTPUT_BUCKET . '.table2a',
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
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);

        $this->assertJobParamsMatches([
            'incremental' => true,
            'columns' => ['Id'],
        ], $jobIds[0]);

        $this->assertJobParamsMatches([
            'incremental' => false,
            'columns' => ['Id2', 'Name2'],
        ], $jobIds[1]);

        $this->assertTablesExists([self::OUTPUT_BUCKET . '.table1a', self::OUTPUT_BUCKET . '.table2a']);
        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a', [
            '"id","name"',
            '"test","test"',
            '"aabb","ccdd"',
        ]);
    }

    public function testTableOutputMappingMissing()
    {
        $root = $this->tmp->getTmpFolder();
        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        $writer = new TableWriter($this->getStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to load table "' . self::OUTPUT_BUCKET . '.table1a": Table "table1a" not found in schema "WORKSPACE_');

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake'
        );
        $tableQueue->waitForAll();
    }

    public function testTableOutputMappingMissingManifest()
    {
        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
            ],
        ];
        $writer = new TableWriter($this->getStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessageRegExp('/Table "table1a" not found in schema "WORKSPACE_\d+"$/');

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake'
        );
        $tableQueue->waitForAll();
    }

    public function testMappingMerge()
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_SNOWFLAKE)->getDataStorage()->getWorkspaceId();

        $root = $this->tmp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($tokenInfo['owner']['defaultBackend'], 'WriterWorkspaceTest');

        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
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
            'workspace-snowflake'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $tableMetadata = $metadata->listTableMetadata(self::OUTPUT_BUCKET . '.table1a');
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

    public function testTableOutputMappingMissingDestinationManifest()
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_SNOWFLAKE)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        $configs = [
            [
                'source' => 'table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ]
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
            'workspace-snowflake'
        );
    }

    public function testTableOutputMappingMissingDestinationNoManifest()
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_SNOWFLAKE)->getDataStorage()->getWorkspaceId();
        $configs = [
            [
                'source' => 'table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ]
        ];
        $writer = new TableWriter($factory);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to resolve destination for output table "table1a".');

        $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-snowflake'
        );
    }

    public function testSnowflakeTableOutputBucketNoDestination()
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_SNOWFLAKE)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($tokenInfo['owner']['defaultBackend'], 'WriterWorkspaceTest');

        $configs = [
            [
                'source' => 'table1a',
            ]
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
            ['mapping' => $configs, 'bucket' => self::OUTPUT_BUCKET],
            ['componentId' => 'foo'],
            'workspace-snowflake'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $this->assertJobParamsMatches([
            'columns' => ['Id', 'Name'],
        ], $jobIds[0]);

        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a', [
            '"id","name"',
            '"test","test"',
            '"aabb","ccdd"',
        ]);
    }

    public function testRedshiftTableOutputMapping()
    {
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_REDSHIFT, 'redshift']);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_REDSHIFT)->getDataStorage()->getWorkspaceId();

        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('redshift', 'WriterWorkspaceTest');
        // snowflake bucket does not work - https://keboola.atlassian.net/browse/KBC-228
        $this->clientWrapper->getBasicClient()->createBucket('WriterWorkspaceTest', 'out', '', 'redshift');
        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
            ],
            [
                'source' => 'table2a',
                'destination' => self::OUTPUT_BUCKET . '.table2a',
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
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables('/', ['mapping' => $configs], ['componentId' => 'foo'], StrategyFactory::WORKSPACE_REDSHIFT);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);

        $this->assertTablesExists([self::OUTPUT_BUCKET . '.table1a', self::OUTPUT_BUCKET . '.table2a']);
        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a', [
            '"id","name"',
            '"test","test"',
            '"aabb","ccdd"',
        ]);
    }

    public function testWriteTableOutputMappingDevMode()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                STORAGE_API_URL,
                STORAGE_API_TOKEN_MASTER,
                null,
            )
        );
        $branchId = $this->createBranch($clientWrapper, self::TEST_BRANCH);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                STORAGE_API_URL,
                STORAGE_API_TOKEN_MASTER,
                $branchId,
            )
        );

        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_SNOWFLAKE)->getDataStorage()->getWorkspaceId();

        $this->prepareWorkspaceWithTables($tokenInfo['owner']['defaultBackend'], 'WriterWorkspaceTest');
        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
                'incremental' => true,
                'columns' => ['Id'],
            ],
            [
                'source' => 'table2a',
                'destination' => self::OUTPUT_BUCKET . '.table2a',
            ],
        ];
        $root = $this->tmp->getTmpFolder();
        $this->tmp->initRunFolder();
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($factory);

        $tableQueue = $writer->uploadTables(
            '/',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            'workspace-snowflake'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $tables = $this->clientWrapper->getBasicClient()->listTables(sprintf(self::BRANCH_BUCKET, $branchId));
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(
            [
                self::BRANCH_BUCKET . '.table1a',
                self::BRANCH_BUCKET . '.table2a',
            ],
            $tableIds
        );
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
    }

    public function testSnowflakeMultipleMappingOfSameSource()
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $factory = $this->getStagingFactory(null, 'json', null, [StrategyFactory::WORKSPACE_SNOWFLAKE, $tokenInfo['owner']['defaultBackend']]);
        // initialize the workspace mock
        $factory->getTableOutputStrategy(StrategyFactory::WORKSPACE_SNOWFLAKE)->getDataStorage()->getWorkspaceId();
        $root = $this->tmp->getTmpFolder();
        // because of https://keboola.atlassian.net/browse/KBC-228 we need to use default backend (or create the
        // target bucket with the same backend)
        $this->prepareWorkspaceWithTables($tokenInfo['owner']['defaultBackend'], 'WriterWorkspaceTest');

        $configs = [
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a',
            ],
            [
                'source' => 'table1a',
                'destination' => self::OUTPUT_BUCKET . '.table1a_2',
            ],
        ];
        file_put_contents($root . '/table1a.manifest', json_encode(['columns' => ['Id', 'Name']]));

        $writer = new TableWriter($factory);
        $tableQueue = $writer->uploadTables('/', ['mapping' => $configs], ['componentId' => 'foo'], 'workspace-snowflake');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);

        $this->assertTablesExists([self::OUTPUT_BUCKET . '.table1a', self::OUTPUT_BUCKET . '.table1a_2']);
        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a', [
            '"id","name"',
            '"test","test"',
            '"aabb","ccdd"',
        ]);
        $this->assertTableRowsEquals(self::OUTPUT_BUCKET . '.table1a_2', [
            '"id","name"',
            '"test","test"',
            '"aabb","ccdd"',
        ]);
    }
}
