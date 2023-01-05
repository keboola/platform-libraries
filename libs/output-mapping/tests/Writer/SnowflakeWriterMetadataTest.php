<?php

namespace Keboola\DockerBundle\Tests;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterMetadataTest;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Metadata;

class SnowflakeWriterMetadataTest extends BaseWriterMetadataTest
{
    private const INPUT_BUCKET = 'in.c-SnowflakeWriterMetadataTest';
    private const OUTPUT_BUCKET = 'out.c-SnowflakeWriterMetadataTest';
    private const FILE_TAG = 'SnowflakeWriterMetadataTest';

    public function setUp(): void
    {
        parent::setUp();
        $this->clearFileUploads([self::FILE_TAG]);
        $this->clearBuckets([self::INPUT_BUCKET, self::OUTPUT_BUCKET]);
        $this->clientWrapper->getBasicClient()->createBucket('SnowflakeWriterMetadataTest', "in", '', 'snowflake');

        $this->backend = 'snowflake';
    }

    public function testMetadataWritingTest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table55.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $config = [
            "mapping" => [
                [
                    "source" => "table55.csv",
                    "destination" => self::INPUT_BUCKET . ".table55",
                    "metadata" => [
                        [
                            "key" => "table.key.one",
                            "value" => "table value one"
                        ],
                        [
                            "key" => "table.key.two",
                            "value" => "table value two"
                        ]
                    ],
                    "column_metadata" => [
                        "Id" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one id"
                            ],
                            [
                                "key" => "column.key.two",
                                "value" => "column value two id"
                            ]
                        ],
                        "Name" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one text"
                            ],
                            [
                                "key" => "column.key.two",
                                "value" => "column value two text"
                            ]
                        ]
                    ]
                ]
            ],
        ];
        $systemMetadata = [
            "componentId" => "testComponent",
            "configurationId" => "metadata-write-test",
            "branchId" => "1234",
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', $config, $systemMetadata, StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());

        $tableMetadata = $metadataApi->listTableMetadata(self::INPUT_BUCKET . '.table55');
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
                'table.key.two' => 'table value two'
            ]
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        $idColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table55.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'column value two id',
            ]
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));

        // check metadata update
        $tableQueue =  $writer->uploadTables('upload', $config, $systemMetadata, 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tableMetadata = $metadataApi->listTableMetadata(self::INPUT_BUCKET . '.table55');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'metadata-write-test';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'testComponent';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    public function testMetadataWritingErrorTest()
    {
        $this->markTestSkipped('Temporary skipped due bug in KBC');
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table55a.csv", "\"Id\",\"Name\"\n\"test\"\n\"aabb\"\n");

        $config = [
            "mapping" => [
                [
                    "source" => "table55a.csv",
                    "destination" => self::INPUT_BUCKET . ".table55a",
                    "column_metadata" => [
                        "NonExistent" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one id",
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $systemMetadata = ["componentId" => "testComponent"];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', $config, $systemMetadata, StrategyFactory::LOCAL);
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to load table ' . self::INPUT_BUCKET . '".table55a": Load error: ' .
            'odbc_execute(): SQL error: Number of columns in file (1) does not match that of the corresponding ' .
            'table (2), use file format option error_on_column_count_mismatch=false to ignore this error');
        $tableQueue->waitForAll();
    }

    public function testConfigRowMetadataWritingTest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table66.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $config = [
            "mapping" => [
                [
                    "source" => "table66.csv",
                    "destination" => self::INPUT_BUCKET . ".table66",
                    "metadata" => [
                        [
                            "key" => "table.key.one",
                            "value" => "table value one"
                        ],
                        [
                            "key" => "table.key.two",
                            "value" => "table value two"
                        ]
                    ],
                    "column_metadata" => [
                        "Id" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one id"
                            ],
                            [
                                "key" => "column.key.two",
                                "value" => "column value two id"
                            ]
                        ],
                        "Name" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one text"
                            ],
                            [
                                "key" => "column.key.two",
                                "value" => "column value two text"
                            ]
                        ]
                    ]
                ]
            ],
        ];
        $systemMetadata = [
            "componentId" => "testComponent",
            "configurationId" => "metadata-write-test",
            "configurationRowId" => "row-1"
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('/upload', $config, $systemMetadata, StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());

        $tableMetadata = $metadataApi->listTableMetadata(self::INPUT_BUCKET . '.table66');
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
                'table.key.two' => 'table value two'
            ]
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        $idColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table66.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'column value two id',
            ]
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));

        // check metadata update
        $tableQueue =  $writer->uploadTables('/upload', $config, $systemMetadata, 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tableMetadata = $metadataApi->listTableMetadata(self::INPUT_BUCKET . '.table66');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configurationRow.id'] = 'row-1';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'metadata-write-test';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'testComponent';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    public function testMetadataWritingTestColumnChange()
    {
        $this->metadataWritingTestColumnChangeTest(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeSpecialDelimiter()
    {
        $this->metadataWritingTestColumnChangeSpecialDelimiter(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeSpecialChars()
    {
        $this->metadataWritingTestColumnChangeSpecialChars(self::INPUT_BUCKET);
    }

    public function testMetadataWritingTestColumnChangeHeadless()
    {
        $this->metadataWritingTestColumnChangeHeadless(self::INPUT_BUCKET);
    }
}
