<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\StorageApi\Metadata;

class SnowflakeWriterMetadataTest extends BaseWriterMetadataTest
{
    public function setUp(): void
    {
        parent::setUp();
        $this->backend = 'snowflake';
    }

    #[NeedsEmptyOutputBucket]
    public function testMetadataWritingTest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table55.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $config = [
            'mapping' => [
                [
                    'source' => 'table55.csv',
                    'destination' => $this->emptyOutputBucketId . '.table55',
                    'metadata' => [
                        [
                            'key' => 'table.key.one',
                            'value' => 'table value one',
                        ],
                        [
                            'key' => 'table.key.two',
                            'value' => 'table value two',
                        ],
                    ],
                    'column_metadata' => [
                        'Id' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one id',
                            ],
                            [
                                'key' => 'column.key.two',
                                'value' => 'column value two id',
                            ],
                        ],
                        'Name' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one text',
                            ],
                            [
                                'key' => 'column.key.two',
                                'value' => 'column value two text',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'branchId' => '1234',
        ];

        $tableQueue = $this->getTableLoader($this->getWorkspaceStagingFactory())->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: $config,
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata($systemMetadata),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
        $metadataApi = new Metadata($this->clientWrapper->getTableAndFileStorageClient());

        $tableMetadata = $metadataApi->listTableMetadata($this->emptyOutputBucketId . '.table55');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'testComponent',
                'KBC.createdBy.configuration.id' => 'metadata-write-test',
                'KBC.lastUpdatedBy.component.id' => 'testComponent',
                'KBC.lastUpdatedBy.configuration.id' => 'metadata-write-test',
                'KBC.createdBy.branch.id' => '1234',
                'KBC.lastUpdatedBy.branch.id' => '1234',
            ],
            'testComponent' => [
                'table.key.one' => 'table value one',
                'table.key.two' => 'table value two',
            ],
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        $idColMetadata = $metadataApi->listColumnMetadata($this->emptyOutputBucketId . '.table55.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'column value two id',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));

        // check metadata update
        $tableQueue = $this->getTableLoader($this->getWorkspaceStagingFactory())->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: $config,
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata($systemMetadata),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tableMetadata = $metadataApi->listTableMetadata($this->emptyOutputBucketId . '.table55');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'metadata-write-test';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'testComponent';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    #[NeedsEmptyOutputBucket]
    public function testMetadataWritingErrorTest(): void
    {
        self::markTestSkipped('Temporary skipped due bug in KBC');
        // @phpstan-ignore-next-line
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table55a.csv', "\"Id\",\"Name\"\n\"test\"\n\"aabb\"\n");

        $config = [
            'mapping' => [
                [
                    'source' => 'table55a.csv',
                    'destination' => $this->emptyOutputBucketId . '.table55a',
                    'column_metadata' => [
                        'NonExistent' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one id',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $systemMetadata = ['componentId' => 'testComponent'];

        $tableQueue = $this->getTableLoader()->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: $config,
                sourcePathPrefix: 'upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata($systemMetadata),
        );
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to load table ' . $this->emptyOutputBucketId .
            '".table55a": Load error: ' .
            'odbc_execute(): SQL error: Number of columns in file (1) does not match that of the corresponding ' .
            'table (2), use file format option error_on_column_count_mismatch=false to ignore this error');
        $tableQueue->waitForAll();
    }

    #[NeedsEmptyOutputBucket]
    public function testConfigRowMetadataWritingTest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/table66.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $config = [
            'mapping' => [
                [
                    'source' => 'table66.csv',
                    'destination' => $this->emptyOutputBucketId . '.table66',
                    'metadata' => [
                        [
                            'key' => 'table.key.one',
                            'value' => 'table value one',
                        ],
                        [
                            'key' => 'table.key.two',
                            'value' => 'table value two',
                        ],
                    ],
                    'column_metadata' => [
                        'Id' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one id',
                            ],
                            [
                                'key' => 'column.key.two',
                                'value' => 'column value two id',
                            ],
                        ],
                        'Name' => [
                            [
                                'key' => 'column.key.one',
                                'value' => 'column value one text',
                            ],
                            [
                                'key' => 'column.key.two',
                                'value' => 'column value two text',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => 'row-1',
        ];

        $tableQueue = $this->getTableLoader($this->getLocalStagingFactory())->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: $config,
                sourcePathPrefix: '/upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata($systemMetadata),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getTableAndFileStorageClient());

        $tableMetadata = $metadataApi->listTableMetadata($this->emptyOutputBucketId . '.table66');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'testComponent',
                'KBC.createdBy.configuration.id' => 'metadata-write-test',
                'KBC.createdBy.configurationRow.id' => 'row-1',
                'KBC.lastUpdatedBy.component.id' => 'testComponent',
                'KBC.lastUpdatedBy.configuration.id' => 'metadata-write-test',
                'KBC.lastUpdatedBy.configurationRow.id' => 'row-1',
            ],
            'testComponent' => [
                'table.key.one' => 'table value one',
                'table.key.two' => 'table value two',
            ],
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        $idColMetadata = $metadataApi->listColumnMetadata($this->emptyOutputBucketId. '.table66.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'column value two id',
            ],
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));

        // check metadata update
        $tableQueue = $this->getTableLoader($this->getLocalStagingFactory())->uploadTables(
            outputStaging: AbstractStrategyFactory::LOCAL,
            configuration: new OutputMappingSettings(
                configuration: $config,
                sourcePathPrefix: '/upload',
                storageApiToken: $this->getLocalStagingFactory()->getClientWrapper()->getToken(),
                isFailedJob: false,
                dataTypeSupport: 'none',
            ),
            systemMetadata: new SystemMetadata($systemMetadata),
        );
        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);

        $tableMetadata = $metadataApi->listTableMetadata($this->emptyOutputBucketId. '.table66');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configurationRow.id'] = 'row-1';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'metadata-write-test';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'testComponent';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    /**
     * @dataProvider incrementalFlagProvider
     */
    #[NeedsEmptyOutputBucket]
    public function testMetadataWritingTestColumnChange(bool $incrementalFlag): void
    {
        $this->metadataWritingTestColumnChangeTest($this->emptyOutputBucketId, $incrementalFlag);
    }

    #[NeedsEmptyOutputBucket]
    public function testMetadataWritingTestColumnChangeSpecialDelimiter(): void
    {
        $this->metadataWritingTestColumnChangeSpecialDelimiter($this->emptyOutputBucketId);
    }

    #[NeedsEmptyOutputBucket]
    public function testMetadataWritingTestColumnChangeSpecialChars(): void
    {
        $this->metadataWritingTestColumnChangeSpecialChars($this->emptyOutputBucketId);
    }

    #[NeedsEmptyOutputBucket]
    public function testMetadataWritingTestColumnChangeHeadless(): void
    {
        $this->metadataWritingTestColumnChangeHeadless($this->emptyOutputBucketId);
    }
}
