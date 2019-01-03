<?php

namespace Keboola\DockerBundle\Tests;

use Keboola\OutputMapping\Writer\Writer;
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
        $this->client->createBucket('docker-test', "in");
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

        $writer = new Writer($this->client, new NullLogger());
        $jobIds = $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $this->assertCount(1, $jobIds);
        $this->client->handleAsyncTasks($jobIds);
        $metadataApi = new Metadata($this->client);

        $tableMetadata = $metadataApi->listTableMetadata('in.c-docker-test.table55');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'testComponent',
                'KBC.createdBy.configuration.id' => 'metadata-write-test',
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
        $jobIds = $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $this->assertCount(1, $jobIds);
        $this->client->handleAsyncTasks($jobIds);

        $tableMetadata = $metadataApi->listTableMetadata('in.c-docker-test.table55');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'metadata-write-test';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'testComponent';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }

    public function testMetadataWritingTestColumnChange()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table88.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $config = [
            "mapping" => [
                [
                    "source" => "table88.csv",
                    "destination" => "in.c-docker-test.table88",
                ],
            ],
        ];
        $systemMetadata = [
            "componentId" => "testComponent",
            "configurationId" => "metadata-write-test"
        ];

        $writer = new Writer($this->client, new NullLogger());
        $jobIds = $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $this->assertCount(1, $jobIds);
        $this->client->waitForJob($jobIds[0]);

        file_put_contents(
            $root . "/upload/table88.csv",
            "\"Id\",\"Name\",\"Foo\"\n\"test\",\"test\",\"bar\"\n\"aabb\",\"ccdd\",\"eeff\"\n"
        );
        $config = [
            "mapping" => [
                [
                    "source" => "table88.csv",
                    "destination" => "in.c-docker-test.table88",
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

        $jobIds = $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $this->assertCount(1, $jobIds);
        $this->client->waitForJob($jobIds[0]);

        $metadataApi = new Metadata($this->client);
        $idColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test.table88.Id');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.one' => 'column value one id2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($idColMetadata));
        $NameColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test.table88.Name');
        $expectedColumnMetadata = [
            'testComponent' => [
                'column.key.two' => 'column value one text2',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($NameColMetadata));
        $FooColMetadata = $metadataApi->listColumnMetadata('in.c-docker-test.table88.Foo');
        $expectedColumnMetadata = [
            'testComponent' => [
                'foo.one' => 'bar one',
            ]
        ];
        $this->assertEquals($expectedColumnMetadata, $this->getMetadataValues($FooColMetadata));
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

        $writer = new Writer($this->client, new NullLogger());
        $jobIds = $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $this->assertCount(1, $jobIds);
        $this->client->handleAsyncTasks($jobIds);

        $metadataApi = new Metadata($this->client);

        $tableMetadata = $metadataApi->listTableMetadata('in.c-docker-test.table66');
        $expectedTableMetadata = [
            'system' => [
                'KBC.createdBy.component.id' => 'testComponent',
                'KBC.createdBy.configuration.id' => 'metadata-write-test',
                'KBC.createdBy.configurationRow.id' => 'row-1',
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
        $jobIds = $writer->uploadTables($root . "/upload", $config, $systemMetadata);
        $this->assertCount(1, $jobIds);
        $this->client->handleAsyncTasks($jobIds);

        $tableMetadata = $metadataApi->listTableMetadata('in.c-docker-test.table66');
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configurationRow.id'] = 'row-1';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.configuration.id'] = 'metadata-write-test';
        $expectedTableMetadata['system']['KBC.lastUpdatedBy.component.id'] = 'testComponent';
        self::assertEquals($expectedTableMetadata, $this->getMetadataValues($tableMetadata));
    }
}
