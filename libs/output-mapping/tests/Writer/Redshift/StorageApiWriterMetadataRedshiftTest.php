<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;

class StorageApiWriterMetadataRedshiftTest extends BaseWriterMetadataTest
{
    private const INPUT_BUCKET = 'in.c-StorageApiSlicedWriterRedshiftTest';
    private const FILE_TAG = 'StorageApiSlicedWriterRedshiftTest';

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([self::INPUT_BUCKET]);
        $this->clearFileUploads([self::FILE_TAG]);
        $this->clientWrapper->getBasicClient()->createBucket(
            'StorageApiSlicedWriterRedshiftTest',
            "in",
            '',
            'redshift'
        );
    }

    public function testMetadataWritingTestColumnChange()
    {
        $root = $this->tmp->getTmpFolder();
        $csv = new CsvFile($root . "/table88a.csv");
        $csv->writeRow(['Id', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTableAsync(self::INPUT_BUCKET, 'table88', $csv);

        $csv = new CsvFile($root . "/upload/table88b.csv");
        $csv->writeRow(['Id', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            "mapping" => [
                [
                    "source" => "table88b.csv",
                    "destination" => self::INPUT_BUCKET . ".table88",
                    "metadata" => [],
                    "column_metadata" => [
                        "Id" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one id2"
                            ],
                        ],
                        "Name" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one text2"
                            ],
                        ],
                        "Foo" => [
                            [
                                "key" => "foo.one",
                                "value" => "bar one",
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', $config, ["componentId" => "testComponent"], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $idColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table88.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $NameColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($NameColMetadata));
        $FooColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($FooColMetadata));
    }

    public function testMetadataWritingTestColumnChangeSpecialDelimiter($backend)
    {
        $root = $this->tmp->getTmpFolder();
        $csv = new CsvFile($root . "/table88a.csv");
        $csv->writeRow(['Id with special chars', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTableAsync(self::INPUT_BUCKET, 'table88', $csv);

        $csv = new CsvFile($root . "/upload/table88b.csv", ';', '\'');
        $csv->writeRow(['Id with special chars', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            "mapping" => [
                [
                    "source" => "table88b.csv",
                    "destination" => self::INPUT_BUCKET . ".table88",
                    "delimiter" => ";",
                    "enclosure" => "'",
                    "metadata" => [],
                    "column_metadata" => [
                        "Name" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one text2"
                            ],
                        ],
                        "Foo" => [
                            [
                                "key" => "foo.one",
                                "value" => "bar one",
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', $config, ["componentId" => "testComponent"], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $nameColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }

    public function testMetadataWritingTestColumnChangeSpecialChars()
    {
        $root = $this->tmp->getTmpFolder();
        $csv = new CsvFile($root . "/table88a.csv");
        $csv->writeRow(['Id with special chars', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTableAsync(self::INPUT_BUCKET, 'table88', $csv);

        $csv = new CsvFile($root . "/upload/table88b.csv");
        $csv->writeRow(['Id with special chars', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            "mapping" => [
                [
                    "source" => "table88b.csv",
                    "destination" => self::INPUT_BUCKET . ".table88",
                    "metadata" => [],
                    "column_metadata" => [
                        "Id with special chars" => [
                            [
                                "key" => "column.key.zero",
                                "value" => "column value on id",
                            ],
                        ],
                        "Name" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one text2",
                            ],
                        ],
                        "Foo" => [
                            [
                                "key" => "foo.one",
                                "value" => "bar one",
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', $config, ["componentId" => "testComponent"], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $idColMetadata = $metadataApi->listColumnMetadata(
            self::INPUT_BUCKET . '.table88.Id_with_special_chars'
        );
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.zero' => 'column value on id',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $nameColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }

    public function testMetadataWritingTestColumnChangeHeadless()
    {
        $root = $this->tmp->getTmpFolder();
        $csv = new CsvFile($root . "/table99a.csv");
        $csv->writeRow(['Id', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->clientWrapper->getBasicClient()->createTableAsync(self::INPUT_BUCKET, 'table99', $csv);

        mkdir($root . "/upload/table99b", 0777, true);
        $csv = new CsvFile($root . "/upload/table99b/slice1.csv");
        $csv->writeRow(['test', 'test', 'bar']);
        $csv = new CsvFile($root . "/upload/table99b/slice2.csv");
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            "mapping" => [
                [
                    "source" => "table99b",
                    "destination" => self::INPUT_BUCKET . ".table99",
                    "columns" => ["Id", "Name", "Foo"],
                    "metadata" => [],
                    "column_metadata" => [
                        "Id" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one id2"
                            ],
                        ],
                        "Name" => [
                            [
                                "key" => "column.key.one",
                                "value" => "column value one text2"
                            ],
                        ],
                        "Foo" => [
                            [
                                "key" => "foo.one",
                                "value" => "bar one",
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('upload', $config, ["componentId" => "testComponent"], StrategyFactory::LOCAL);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->clientWrapper->getBasicClient());
        $idColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table99.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $nameColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table99.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata(self::INPUT_BUCKET . '.table99.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }
}
