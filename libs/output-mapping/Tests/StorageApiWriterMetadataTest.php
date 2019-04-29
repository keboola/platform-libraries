<?php

namespace Keboola\DockerBundle\Tests;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class StorageApiWriterMetadataTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Temp
     */
    private $tmp;

    /**
     * Transform metadata into a key-value array
     * @param $metadata
     * @return array
     */
    private function getMetadataValues($metadata)
    {
        $result = [];
        foreach ($metadata as $item) {
            $result[$item['provider']][$item['key']] = $item['value'];
        }
        return $result;
    }

    public function setUp()
    {
        // Create folders
        $this->tmp = new Temp();
        $this->tmp->initRunFolder();
        $root = $this->tmp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'upload');
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'download');

        $this->client = new Client([
            'url' => STORAGE_API_URL,
            'token' => STORAGE_API_TOKEN,
        ]);

        try {
            $this->client->dropBucket('in.c-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        try {
            $this->client->dropBucket('in.c-docker-test-backend', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->client->createBucket('docker-test', "in", '', 'snowflake');
    }

    public function tearDown()
    {
        try {
            $this->client->dropBucket('in.c-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        // Delete local files
        $this->tmp = null;
    }

    public function testMetadataWritingTest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table55.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $config = [
            "mapping" => [
                [
                    "source" => "table55.csv",
                    "destination" => "in.c-docker-test.table55",
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
            "configurationId" => "metadata-write-test"
        ];

        $writer = new TableWriter($this->client, new NullLogger());
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $metadataApi = new Metadata($this->client);

        $tableMetadata = $metadataApi->listTableMetadata('in.c-docker-test.table55');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'testComponent',
                'KBC.createdBy.configuration.id' => 'metadata-write-test',
                'KBC.lastUpdatedBy.component.id' => 'testComponent',
                'KBC.lastUpdatedBy.configuration.id' => 'metadata-write-test',
            ],
            'testComponent' => [
                'table.key.one' => 'table value one',
                'table.key.two' => 'table value two'
            ]
        ];
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));

        $idColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test.table55.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'column value two id',
            ]
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));

        // check metadata update
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tableMetadata = $metadataApi->listTableMetadata('in.c-docker-test.table55');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'metadata-write-test';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'testComponent';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    public function testMetadataWritingErrorTest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table55a.csv", "\"Id\",\"Name\"\n\"test\"\n\"aabb\"\n");

        $config = [
            "mapping" => [
                [
                    "source" => "table55a.csv",
                    "destination" => "in.c-docker-test.table55a",
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

        $writer = new TableWriter($this->client, new NullLogger());
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to load table "in.c-docker-test.table55a": Load error: ' .
            'odbc_execute(): SQL error: Number of columns in file (1) does not match that of the corresponding ' .
            'table (2), use file format option error_on_column_count_mismatch=false to ignore this error');
        $tableQueue->waitForAll();
    }

    /**
     * @dataProvider backendProvider
     * @param string $backend
     * @throws ClientException
     * @throws \Keboola\Csv\Exception
     */
    public function testMetadataWritingTestColumnChange($backend)
    {
        $this->client->createBucket('docker-test-backend', "in", '', $backend);
        $root = $this->tmp->getTmpFolder();
        $csv = new CsvFile($root . "/table88a.csv");
        $csv->writeRow(['Id', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->client->createTableAsync('in.c-docker-test-backend', 'table88', $csv);

        $csv = new CsvFile($root . "/upload/table88b.csv");
        $csv->writeRow(['Id', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            "mapping" => [
                [
                    "source" => "table88b.csv",
                    "destination" => "in.c-docker-test-backend.table88",
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
        $writer = new TableWriter($this->client, new NullLogger());
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, ["componentId" => "testComponent"]);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->client);
        $idColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table88.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $NameColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($NameColMetadata));
        $FooColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($FooColMetadata));
    }

    /**
     * @dataProvider backendProvider
     * @param string $backend
     * @throws ClientException
     * @throws \Keboola\Csv\Exception
     */
    public function testMetadataWritingTestColumnChangeSpecialDelimiter($backend)
    {
        $this->client->createBucket('docker-test-backend', "in", '', $backend);
        $root = $this->tmp->getTmpFolder();
        $csv = new CsvFile($root . "/table88a.csv");
        $csv->writeRow(['Id with special chars', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->client->createTableAsync('in.c-docker-test-backend', 'table88', $csv);

        $csv = new CsvFile($root . "/upload/table88b.csv", ';', '\'');
        $csv->writeRow(['Id with special chars', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            "mapping" => [
                [
                    "source" => "table88b.csv",
                    "destination" => "in.c-docker-test-backend.table88",
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
        $writer = new TableWriter($this->client, new NullLogger());
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, ["componentId" => "testComponent"]);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->client);
        $nameColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }

    /**
     * @dataProvider backendProvider
     * @param string $backend
     * @throws ClientException
     * @throws \Keboola\Csv\Exception
     */
    public function testMetadataWritingTestColumnChangeSpecialChars($backend)
    {
        $this->client->createBucket('docker-test-backend', "in", '', $backend);
        $root = $this->tmp->getTmpFolder();
        $csv = new CsvFile($root . "/table88a.csv");
        $csv->writeRow(['Id with special chars', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->client->createTableAsync('in.c-docker-test-backend', 'table88', $csv);

        $csv = new CsvFile($root . "/upload/table88b.csv");
        $csv->writeRow(['Id with special chars', 'Name', 'Foo']);
        $csv->writeRow(['test', 'test', 'bar']);
        $csv->writeRow(['aabb', 'ccdd', 'baz']);
        unset($csv);
        $config = [
            "mapping" => [
                [
                    "source" => "table88b.csv",
                    "destination" => "in.c-docker-test-backend.table88",
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
        $writer = new TableWriter($this->client, new NullLogger());
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, ["componentId" => "testComponent"]);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->client);
        $nameColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }

    /**
     * @dataProvider backendProvider
     * @param $backend
     * @throws ClientException
     * @throws \Keboola\Csv\Exception
     */
    public function testMetadataWritingTestColumnChangeHeadless($backend)
    {
        $this->client->createBucket('docker-test-backend', "in", '', $backend);
        $root = $this->tmp->getTmpFolder();
        $csv = new CsvFile($root . "/table99a.csv");
        $csv->writeRow(['Id', 'Name']);
        $csv->writeRow(['test', 'test']);
        $csv->writeRow(['aabb', 'ccdd']);
        $this->client->createTableAsync('in.c-docker-test-backend', 'table99', $csv);

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
                    "destination" => "in.c-docker-test-backend.table99",
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
        $writer = new TableWriter($this->client, new NullLogger());
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, ["componentId" => "testComponent"]);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->client);
        $idColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table99.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $nameColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table99.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($nameColMetadata));
        $fooColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test-backend.table99.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($fooColMetadata));
    }

    public function testConfigRowMetadataWritingTest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table66.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $config = [
            "mapping" => [
                [
                    "source" => "table66.csv",
                    "destination" => "in.c-docker-test.table66",
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

        $writer = new TableWriter($this->client, new NullLogger());
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $metadataApi = new Metadata($this->client);

        $tableMetadata = $metadataApi->listTableMetadata('in.c-docker-test.table66');
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

        $idColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test.table66.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id',
                'column.key.two' => 'column value two id',
            ]
        ];
        self::assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));

        // check metadata update
        $tableQueue =  $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tableMetadata = $metadataApi->listTableMetadata('in.c-docker-test.table66');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configurationRow.id'] = 'row-1';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'metadata-write-test';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'testComponent';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    public function backendProvider()
    {
        return [['snowflake'], ['redshift']];
    }
}
