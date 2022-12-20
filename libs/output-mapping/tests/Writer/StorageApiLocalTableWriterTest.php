<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Psr\Log\Test\TestLogger;

class StorageApiLocalTableWriterTest extends BaseWriterTest
{
    use CreateBranchTrait;

    private const OUTPUT_BUCKET = 'out.c-StorageApiLocalTableWriterTest';
    private const BUCKET_NAME = 'StorageApiLocalTableWriterTest';

    const DEFAULT_SYSTEM_METADATA = ['componentId' => 'foo'];

    public function setUp()
    {
        parent::setUp();
        $this->clearBuckets([
            self::OUTPUT_BUCKET,
        ]);
        $this->clientWrapper->getBasicClient()->createBucket('StorageApiLocalTableWriterTest', 'out');
    }

    public function testWriteTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1a.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/upload/table2a.csv", "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n");

        $configs = [
            [
                "source" => "table1a.csv",
                "destination" => self::OUTPUT_BUCKET . ".table1a"
            ],
            [
                "source" => "table2a.csv",
                "destination" => self::OUTPUT_BUCKET . ".table2a"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('upload', ['mapping' => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals([self::OUTPUT_BUCKET . '.table1a', self::OUTPUT_BUCKET . '.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);

        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals([], $file['tags']);

        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tableQueue->getTableResult()->getTables());
        self::assertCount(2, $tables);

        /** @var TableInfo[] $tables */
        $tables = iterator_to_array($tableQueue->getTableResult()->getTables());
        self::assertCount(2, $tables);

        $tableIds = array_map(function ($table) {
            return $table->getId();
        }, $tables);

        sort($tableIds);
        self::assertSame([
            self::OUTPUT_BUCKET . '.table1a',
            self::OUTPUT_BUCKET . '.table2a',
        ], $tableIds);
    }

    public function testWriteTableTagStagingFiles()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1a.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/upload/table2a.csv", "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n");

        $configs = [
            [
                "source" => "table1a.csv",
                "destination" => self::OUTPUT_BUCKET . ".table1a"
            ],
            [
                "source" => "table2a.csv",
                "destination" => self::OUTPUT_BUCKET . ".table2a"
            ]
        ];

        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $client = $this->getMockBuilder(Client::class)
            ->setConstructorArgs([[
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN,
            ]])
            ->setMethods(['verifyToken'])
            ->getMock();
        $tokenInfo['owner']['features'][] = 'tag-staging-files';
        $client->method('verifyToken')->willReturn($tokenInfo);
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('getBasicClient')->willReturn($client);
        $writer = new TableWriter($this->getStagingFactory($clientWrapper));

        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);

        $this->assertTablesExists(
            self::OUTPUT_BUCKET,
            [
                self::OUTPUT_BUCKET . '.table1a',
                self::OUTPUT_BUCKET . '.table2a',
            ]
        );

        $job = $this->clientWrapper->getBasicClient()->getJob($jobIds[0]);
        $fileId = $job['operationParams']['source']['fileId'];
        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals(
            ['componentId: foo'],
            $file['tags']
        );
    }

    public function testWriteTableOutputMappingDevMode()
    {
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, null),
        );
        $branchId = $this->createBranch($this->clientWrapper, 'dev-123');
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, $branchId),
        );

        $root = $this->tmp->getTmpFolder();
        $this->tmp->initRunFolder();
        file_put_contents($root . "/upload/table11a.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/upload/table21a.csv", "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n");

        $configs = [
            [
                "source" => "table11a.csv",
                "destination" => self::OUTPUT_BUCKET . ".table11a"
            ],
            [
                "source" => "table21a.csv",
                "destination" => self::OUTPUT_BUCKET . ".table21a"
            ]
        ];
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $tables = $this->clientWrapper->getBasicClient()->listTables(
            sprintf('out.c-%s-' . self::BUCKET_NAME, $branchId),
        );
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(
            [
                sprintf('out.c-%s-' . self::BUCKET_NAME . '.table11a', $branchId),
                sprintf('out.c-%s-' . self::BUCKET_NAME . '.table21a', $branchId),
            ],
            $tableIds
        );
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
    }

    public function testWriteTableOutputMappingExistingTable()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table21.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table21.csv",
                "destination" => self::OUTPUT_BUCKET . ".table21"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        // And again
        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $this->assertEquals(self::OUTPUT_BUCKET . '.table21', $tables[0]["id"]);
        $this->assertNotEmpty($jobIds[0]);
    }

    public function testWriteTableOutputMappingWithoutCsv()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table31", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table31",
                "destination" => self::OUTPUT_BUCKET . ".table31"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $this->assertEquals(self::OUTPUT_BUCKET . '.table31', $tables[0]["id"]);
    }

    public function testWriteTableOutputMappingEmptyFile()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table41", "");

        $configs = [
            [
                "source" => "table41",
                "destination" => self::OUTPUT_BUCKET . ".table41"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue = $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Failed to load table "' . self::OUTPUT_BUCKET . '.table41": There are no data in import file');
        $tableQueue->waitForAll();
    }

    public function testWriteTableOutputMappingAndManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/table2.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/table2.csv.manifest",
            '{"destination": "' . self::OUTPUT_BUCKET . '.table2","primary_key": ["Id"]}'
        );

        $configs = [
            [
                "source" => "table2.csv",
                "destination" => self::OUTPUT_BUCKET . ".table",
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $this->assertEquals(self::OUTPUT_BUCKET . '.table', $tables[0]["id"]);
        $this->assertEquals(['Id'], $tables[0]["primaryKey"]);
    }

    public function testWriteTableInvalidManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . self::OUTPUT_BUCKET . '.table3b.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . 'upload/' . self::OUTPUT_BUCKET . '.table3b.csv.manifest',
            '{"destination": "' . self::OUTPUT_BUCKET . '.table3","primary_key": "Id"}'
        );

        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
            $this->fail('Invalid table manifest must cause exception');
        } catch (InvalidOutputException $e) {
            $this->assertContains('Invalid type for path', $e->getMessage());
        }
    }

    public function testWriteTableManifestCsvDefaultBackend()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/" . self::OUTPUT_BUCKET . ".table3c.csv",
            "'Id'\t'Name'\n'test'\t'test''s'\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/" . self::OUTPUT_BUCKET . ".table3c.csv.manifest",
            '{"destination": "' . self::OUTPUT_BUCKET . '.table3c","delimiter": "\t","enclosure": "\'"}'
        );

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $this->assertEquals(self::OUTPUT_BUCKET . '.table3c', $tables[0]["id"]);
        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable(self::OUTPUT_BUCKET . '.table3c', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertCount(1, $table);
        $this->assertCount(2, $table[0]);
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('test', $table[0]['Id']);
        $this->assertEquals('test\'s', $table[0]['Name']);
    }

    public function testWriteTableOrphanedManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/table.csv.manifest",
            '{"destination": "' . self::OUTPUT_BUCKET . '.table3e","primary_key": ["Id", "Name"]}'
        );
        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
            $this->fail("Orphaned manifest must fail");
        } catch (InvalidOutputException $e) {
            $this->assertContains('Found orphaned table manifest: "table.csv.manifest"', $e->getMessage());
        }
    }

    public function testWriteTableOutputMappingMissing()
    {
        $configs = [
            [
                "source" => "table81.csv",
                "destination" => self::OUTPUT_BUCKET . ".table81",
            ]
        ];
        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
            $this->fail("Missing table file must fail");
        } catch (InvalidOutputException $e) {
            $this->assertContains('Table sources not found: "table81.csv"', $e->getMessage());
            $this->assertEquals(404, $e->getCode());
        }
    }

    public function testWriteTableMetadataMissing()
    {
        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('/upload', [], [], 'local');
            self::fail("Missing metadata must fail.");
        } catch (OutputOperationException $e) {
            self::assertContains('Component Id must be set', $e->getMessage());
        }
    }

    public function testWriteTableIncrementalWithDeleteDefault()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table51.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table51.csv",
                "destination" => self::OUTPUT_BUCKET . ".table51",
                "delete_where_column" => "Id",
                "delete_where_values" => ["aabb"],
                "delete_where_operator" => "eq",
                "incremental" => true
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->clientWrapper->getBasicClient()->handleAsyncTasks($jobIds);

        // And again, check first incremental table
        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->clientWrapper->getBasicClient()->handleAsyncTasks($jobIds);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $exporter->exportTable(self::OUTPUT_BUCKET . ".table51", $root . DIRECTORY_SEPARATOR . "download.csv", []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($root . DIRECTORY_SEPARATOR . "download.csv"));
        usort($table, function ($a, $b) {
            return strcasecmp($a['Id'], $b['Id']);
        });
        $this->assertCount(3, $table);
        $this->assertCount(2, $table[0]);
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('aabb', $table[0]['Id']);
        $this->assertEquals('ccdd', $table[0]['Name']);
        $this->assertEquals('test', $table[1]['Id']);
        $this->assertEquals('test', $table[1]['Name']);
        $this->assertEquals('test', $table[2]['Id']);
        $this->assertEquals('test', $table[2]['Name']);
    }

    public function testWriteTableToDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table71.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table71.csv.manifest", '{}');

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['bucket' => self::OUTPUT_BUCKET],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->assertEquals(1, $tableQueue->getTaskCount());

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);

        $this->assertEquals(self::OUTPUT_BUCKET . '.table71', $tables[0]["id"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table71');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);
    }

    public function testWriteTableManifestWithDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table6.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table6.csv.manifest", '{"primary_key": ["Id", "Name"]}');

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['bucket' => self::OUTPUT_BUCKET],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);
        $this->assertEquals(self::OUTPUT_BUCKET . '.table6', $tables[0]["id"]);
        $this->assertEquals(["Id", "Name"], $tables[0]["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithPk()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table16.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table16.csv",
                        "destination" => self::OUTPUT_BUCKET . ".table16",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table16");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithPkOverwrite()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table15.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue = $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table15.csv",
                        "destination" => self::OUTPUT_BUCKET . ".table15",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $tableQueue->waitForAll();

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            [
                "mapping" => [
                    [
                        "source" => "table15.csv",
                        "destination" => self::OUTPUT_BUCKET . ".table15",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table15");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithPkMismatch()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table14.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table14.csv",
                        "destination" => self::OUTPUT_BUCKET . ".table14",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table14");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);

        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables(
                'upload',
                [
                    "mapping" => [
                        [
                            "source" => "table14.csv",
                            "destination" => self::OUTPUT_BUCKET . ".table14",
                            "primary_key" => ["Id", "Name"]
                        ]
                    ]
                ],
                ['componentId' => 'foo'],
                'local'
            );
            $this->fail("Exception not caught");
        } catch (InvalidOutputException $e) {
            $this->assertEquals(
                'Output mapping does not match destination table: primary key "Id, Name" does not match "Id" in "' . self::OUTPUT_BUCKET . '.table14".',
                $e->getMessage()
            );
        }
    }

    public function testWriteTableOutputMappingWithPkMismatchWhitespace()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table13.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table13.csv",
                        "destination" => self::OUTPUT_BUCKET . ".table13",
                        "primary_key" => ["Id "]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table13");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);

        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables(
                'upload',
                [
                    "mapping" => [
                        [
                            "source" => "table13.csv",
                            "destination" => self::OUTPUT_BUCKET . ".table13",
                            "primary_key" => ["Id ", "Name "]
                        ]
                    ]
                ],
                ['componentId' => 'foo'],
                'local'
            );
            $this->fail("Exception not caught");
        } catch (InvalidOutputException $e) {
            $this->assertEquals(
                'Output mapping does not match destination table: primary key "Id, Name" does not match "Id" in "' . self::OUTPUT_BUCKET . '.table13".',
                $e->getMessage()
            );
        }
    }

    public function testWriteTableOutputMappingWithEmptyStringPk()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table12.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $testLogger = new TestLogger();
        $writer = new TableWriter($this->getStagingFactory(null, 'json', $testLogger));
        $tableQueue = $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table12.csv",
                        "destination" => self::OUTPUT_BUCKET . ".table12",
                        "primary_key" => []
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $tableQueue->waitForAll();
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table12");
        $this->assertEquals([], $tableInfo["primaryKey"]);

        $writer = new TableWriter($this->getStagingFactory(null, 'json', $testLogger));
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table12.csv",
                        "destination" => self::OUTPUT_BUCKET . ".table12",
                        "primary_key" => [""]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->clientWrapper->getBasicClient()->handleAsyncTasks($jobIds);
        $this->assertFalse($testLogger->hasWarningThatContains("Output mapping does not match destination table"));
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table12");
        $this->assertEquals([], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithEmptyStringPkInManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table11.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $testLogger = new TestLogger();
        $writer = new TableWriter($this->getStagingFactory(null, 'json', $testLogger));
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table11.csv",
                        "destination" => self::OUTPUT_BUCKET . ".table11",
                        "primary_key" => []
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $writer = new TableWriter($this->getStagingFactory(null, 'json', $testLogger));
        file_put_contents(
            $root . "/upload/table11.csv.manifest",
            '{"destination": "' . self::OUTPUT_BUCKET . '.table11","primary_key": [""]}'
        );
        $tableQueue =  $writer->uploadTables('upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->assertFalse(
            $testLogger->hasWarningThatContains(
                "Output mapping does not match destination table: primary key '' does not match '' in '" . self::OUTPUT_BUCKET . ".table9'."
            )
        );
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . ".table11");
        $this->assertEquals([], $tableInfo["primaryKey"]);
    }

    public function testWriteTableColumnsOverwrite()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/" . self::OUTPUT_BUCKET . ".table10.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        $writer = new TableWriter($this->getStagingFactory());
        $configuration = [
            'mapping' => [
                [
                    'source' => self::OUTPUT_BUCKET . '.table10.csv',
                    'destination' => self::OUTPUT_BUCKET . '.table10',
                ]
            ]
        ];
        $tableQueue = $writer->uploadTables('upload', $configuration, ['componentId' => 'foo'], 'local');
        $tableQueue->waitForAll();

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);

        $this->assertEquals(self::OUTPUT_BUCKET . '.table10', $tables[0]["id"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table10');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);

        file_put_contents($root . "/upload/" . self::OUTPUT_BUCKET . ".table10.csv", "\"foo\",\"bar\"\n\"baz\",\"bat\"\n");
        $writer = new TableWriter($this->getStagingFactory());
        $configuration = [
            'mapping' => [
                [
                    'source' => self::OUTPUT_BUCKET . '.table10.csv',
                    'destination' => self::OUTPUT_BUCKET . '.table10',
                    'columns' => ['Boing', 'Tschak']
                ]
            ]
        ];
        $tableQueue = $writer->uploadTables('upload', $configuration, ['componentId' => 'foo'], 'local');

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessageRegExp('/Some columns are missing in the csv file. Missing columns: id,name./i');
        $tableQueue->waitForAll();
    }

    public function testWriteMultipleErrors()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/" . self::OUTPUT_BUCKET . ".table10a.csv", "\"id\",\"name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/" . self::OUTPUT_BUCKET . ".table10b.csv", "\"foo\",\"bar\"\n\"baz\",\"bat\"\n");
        $writer = new TableWriter($this->getStagingFactory());
        $configuration = [
            'mapping' => [
                [
                    'source' => self::OUTPUT_BUCKET . '.table10a.csv',
                    'destination' => self::OUTPUT_BUCKET . '.table10a',
                ],
                [
                    'source' => self::OUTPUT_BUCKET . '.table10b.csv',
                    'destination' => self::OUTPUT_BUCKET . '.table10b',
                ],
            ]
        ];

        $tableQueue =  $writer->uploadTables('upload', $configuration, ['componentId' => 'foo'], 'local');
        $tableQueue->waitForAll();

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(2, $tables);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table10a');
        $this->assertEquals(["id", "name"], $tableInfo["columns"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable(self::OUTPUT_BUCKET . '.table10b');
        $this->assertEquals(["foo", "bar"], $tableInfo["columns"]);

        $writer = new TableWriter($this->getStagingFactory());
        $configuration = [
            'mapping' => [
                [
                    'source' => self::OUTPUT_BUCKET . '.table10a.csv',
                    'destination' => self::OUTPUT_BUCKET . '.table10a',
                    'columns' => ['Boing', 'Tschak'],
                ],
                [
                    'source' => self::OUTPUT_BUCKET . '.table10b.csv',
                    'destination' => self::OUTPUT_BUCKET . '.table10b',
                    'columns' => ['bum', 'tschak'],
                ],
            ],
        ];
        $tableQueue =  $writer->uploadTables('upload', $configuration, ['componentId' => 'foo'], 'local');
        try {
            $tableQueue->waitForAll();
            $this->fail("Must raise exception");
        } catch (InvalidOutputException $e) {
            $this->assertContains(
                'Failed to load table "' . self::OUTPUT_BUCKET . '.table10a": Some columns are ' .
                'missing in the csv file. Missing columns: id,name. Expected columns: id,name. Please check if the ' .
                'expected delimiter "," is used in the csv file.',
                $e->getMessage()
            );
            $this->assertContains(
                'Failed to load table "' . self::OUTPUT_BUCKET . '.table10b": Some columns are ' .
                'missing in the csv file. Missing columns: foo,bar. Expected columns: foo,bar. Please check if the ' .
                'expected delimiter "," is used in the csv file.',
                $e->getMessage()
            );
        }
    }

    public function testWriteTableExistingBucketDevModeNoDev()
    {
        $root = $this->tmp->getTmpFolder();
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, null),
        );
        $branchId = $this->createBranch($this->clientWrapper, 'dev-123');
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, $branchId),
        );

        file_put_contents($root . '/upload/table21.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table21.csv',
                'destination' => self::OUTPUT_BUCKET . '.table21'
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            StrategyFactory::LOCAL
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        // drop the dev branch metadata
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $branchBucketId = sprintf('out.c-%s-%s', $branchId, self::BUCKET_NAME);
        foreach ($metadata->listBucketMetadata($branchBucketId) as $metadatum) {
            if (($metadatum['key'] === 'KBC.createdBy.branch.id') || ($metadatum['key'] === 'KBC.lastUpdatedBy.branch.id')) {
                $metadata->deleteBucketMetadata($branchBucketId, $metadatum['id']);
            }
        }
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Trying to create a table in the development bucket ' .
            '"' . $branchBucketId . '" on branch "dev-123" (ID "%s"), but the bucket is not assigned ' .
            'to any development branch.',
            $branchId
        ));
        $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            StrategyFactory::LOCAL
        );
    }

    public function testWriteTableExistingBucketDevModeDifferentDev()
    {
        $root = $this->tmp->getTmpFolder();
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, null),
        );
        $branchId = $this->createBranch($this->clientWrapper, 'dev-123');
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, $branchId),
        );

        file_put_contents($root . '/upload/table21.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table21.csv',
                'destination' => self::OUTPUT_BUCKET . '.table21',
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo', 'branchId' => $branchId],
            StrategyFactory::LOCAL
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        // drop the dev branch metadata and create bucket metadata referencing a different branch
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        $branchBucketId = sprintf('out.c-%s-%s', $branchId, self::BUCKET_NAME);
        foreach ($metadata->listBucketMetadata($branchBucketId) as $metadatum) {
            if (($metadatum['key'] === 'KBC.createdBy.branch.id') || ($metadatum['key'] === 'KBC.lastUpdatedBy.branch.id')) {
                $metadata->deleteBucketMetadata($branchBucketId, $metadatum['id']);
                $metadata->postBucketMetadata(
                    $branchBucketId,
                    'system',
                    [
                        [
                            'key' => $metadatum['key'],
                            'value' => '12345',
                        ],
                    ]
                );
            }
        }

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Trying to create a table in the development bucket ' .
            '"' . $branchBucketId . '" on branch "dev-123" (ID "%s"). ' .
            'The bucket metadata marks it as assigned to branch with ID "12345".',
            $branchId
        ));
        $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            StrategyFactory::LOCAL
        );
    }

    /**
     * @dataProvider provideAllowedDestinationConfigurations
     */
    public function testAllowedDestinationConfigurations(
        $manifest,
        $defaultBucket,
        $mapping,
        $expectedTables,
        $isFailedJob = false
    ) {
        $root = $this->tmp->getTmpFolder() . '/upload/';

        file_put_contents($root . 'table.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        if ($manifest !== null) {
            file_put_contents($root . 'table.csv.manifest', json_encode($manifest));
        }

        $tableWriter = new TableWriter($this->getStagingFactory());

        $queue = $tableWriter->uploadTables(
            'upload',
            ['bucket' => $defaultBucket, 'mapping' => $mapping],
            ['componentId' => 'foo'],
            'local',
            false,
            $isFailedJob
        );
        $queue->waitForAll();

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $tablesIds = array_map(function (array $table) {
            return $table['id'];
        }, $tables);
        self::assertSame($expectedTables, $tablesIds);
    }

    public function provideAllowedDestinationConfigurations()
    {
        return [
            'table ID in mapping' => [
                'manifest' => null,
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => self::OUTPUT_BUCKET . '.tableA']
                ],
                'expectedTables' => [self::OUTPUT_BUCKET . '.tableA'],
            ],

            'table ID in manifest' => [
                'manifest' => ['destination' => self::OUTPUT_BUCKET . '.tableA'],
                'defaultBucket' => null,
                'mapping' => null,
                'expectedTables' => [self::OUTPUT_BUCKET . '.tableA'],
            ],

            'table name in manifest with bucket' => [
                'manifest' => ['destination' => 'tableA'],
                'defaultBucket' => self::OUTPUT_BUCKET,
                'mapping' => null,
                'expectedTables' => [self::OUTPUT_BUCKET . '.tableA'],
            ],

            'no destination in manifest with bucket' => [
                'manifest' => ['columns' => ['Id', 'Name']],
                'defaultBucket' => self::OUTPUT_BUCKET,
                'mapping' => null,
                'expectedTables' => [self::OUTPUT_BUCKET . '.table'],
            ],

            'table ID in mapping overrides manifest' => [
                'manifest' => ['destination' => self::OUTPUT_BUCKET . '.tableA'],
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => self::OUTPUT_BUCKET . '.table1']
                ],
                'expectedTables' => [self::OUTPUT_BUCKET . '.table1'],
            ],

            'multiple mappings of the same source' => [
                'manifest' => null,
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => self::OUTPUT_BUCKET . '.table1'],
                    ['source' => 'table.csv', 'destination' => self::OUTPUT_BUCKET . '.table2']
                ],
                'expectedTables' => [self::OUTPUT_BUCKET . '.table1', self::OUTPUT_BUCKET . '.table2'],
            ],

            'failed job with write-alwways flag' => [
                'manifest' => null,
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => self::OUTPUT_BUCKET . '.table1', 'write_always' => false],
                    ['source' => 'table.csv', 'destination' => self::OUTPUT_BUCKET . '.table2', 'write_always' => true]
                ],
                'expectedTables' => [self::OUTPUT_BUCKET . '.table2'],
                true
            ],
        ];
    }

    /**
     * @dataProvider provideForbiddenDestinationConfigurations
     */
    public function testForbiddenDestinationConfigurations($manifest, $defaultBucket, $mapping, $expectedError)
    {
        $root = $this->tmp->getTmpFolder() . '/upload/';

        file_put_contents($root . 'table.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        if ($manifest !== null) {
            file_put_contents($root . 'table.csv.manifest', json_encode($manifest));
        }

        $tableWriter = new TableWriter($this->getStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedError);

        $tableQueue = $tableWriter->uploadTables('upload', ['bucket' => $defaultBucket, 'mapping' => $mapping], ['componentId' => 'foo'], 'local');
        $tableQueue->waitForAll();
    }

    public function provideForbiddenDestinationConfigurations()
    {
        return [
            'no destination in manifest without bucket' => [
                'manifest' => ['columns' => ['Id', 'Name']],
                'defaultBucket' => null,
                'mapping' => null,
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
            ],

            'table name in mapping is not accepted' => [
                'manifest' => null,
                'defaultBucket' => null,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => 'table']
                ],
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
            ],

            'table name in mapping does not combine with default bucket' => [
                'manifest' => null,
                'defaultBucket' => self::OUTPUT_BUCKET,
                'mapping' => [
                    ['source' => 'table.csv', 'destination' => 'table'],
                ],
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
            ],

            'table name in manifest without bucket' => [
                'manifest' => ['destination' => 'table'],
                'defaultBucket' => null,
                'mapping' => null,
                'expectedError' => 'Failed to resolve destination for output table "table.csv".',
            ],
        ];
    }

    /**
     * @dataProvider provideWriteTableBareAllowedVariants
     */
    public function testWriteTableBareAllowedVariants($fileName, $defaultBucket, $expectedTableName)
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/' . $fileName, "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $logger = new TestLogger();
        $writer = new TableWriter($this->getStagingFactory(null, 'json', $logger));

        $tableQueue =  $writer->uploadTables('/upload', ['bucket' => $defaultBucket], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        self::assertTrue($logger->hasWarningThatContains('This behaviour was DEPRECATED and will be removed in the future.'));

        $tables = $this->clientWrapper->getBasicClient()->listTables(self::OUTPUT_BUCKET);
        $this->assertCount(1, $tables);

        $this->assertEquals($expectedTableName, $tables[0]['id']);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable($expectedTableName);
        $this->assertEquals(['Id', 'Name'], $tableInfo['columns']);
    }

    public function provideWriteTableBareAllowedVariants()
    {
        return [
            'table ID as filename without default bucket' => [
                'filename' => self::OUTPUT_BUCKET . '.table41.csv',
                'bucketName' => null,
                'expectedTableName' => self::OUTPUT_BUCKET . '.table41',
            ],

            'table ID as filename with default bucket' => [
                'filename' => self::OUTPUT_BUCKET . '.table42.csv',
                'bucketName' => 'out.c-bucket-is-not-used',
                'expectedTableName' => self::OUTPUT_BUCKET . '.table42',
            ],

            'table name as filename with default bucket' => [
                'filename' => 'table43.csv',
                'bucketName' => self::OUTPUT_BUCKET,
                'expectedTableName' => self::OUTPUT_BUCKET . '.table43',
            ],
        ];
    }

    /**
     * @dataProvider provideWriteTableBareForbiddenVariants
     */
    public function testWriteTableBareForbiddenVariants($fileName, $defaultBucket, $expectedError)
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/' . $fileName, "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $logger = new TestLogger();
        $writer = new TableWriter($this->getStagingFactory(null, 'json', $logger));

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedError);

        $writer->uploadTables('/upload', ['bucket' => $defaultBucket], ['componentId' => 'foo'], 'local');
    }

    public function provideWriteTableBareForbiddenVariants()
    {
        return [
            'table name as filename without default bucket' => [
                'filename' => 'table51.csv',
                'bucketName' => null,
                'expectedError' => 'Failed to resolve destination for output table "table51.csv".',
            ],
        ];
    }

    public function testLocalTableUploadRequiresComponentId()
    {
        $tableWriter = new TableWriter($this->getStagingFactory());

        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');

        $tableWriter->uploadTables('upload', [], [], 'local');
    }

    public function testLocalTableUploadChecksForOrphanedManifests()
    {
        $root = $this->tmp->getTmpFolder() . '/upload/';
        file_put_contents($root . 'table.csv.manifest', json_encode([]));

        $tableWriter = new TableWriter($this->getStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Found orphaned table manifest: "table.csv.manifest"');

        $tableWriter->uploadTables('upload', [], ['componentId' => 'foo'], 'local');
    }

    public function testLocalTableUploadChecksForUnusedMappingEntries()
    {
        $tableWriter = new TableWriter($this->getStagingFactory());

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Table sources not found: "unknown.csv"');

        $tableWriter->uploadTables('upload', [
            'mapping' => [
                [
                    'source' => 'unknown.csv',
                    'destination' => 'unknown',
                ]
            ]
        ], ['componentId' => 'foo'], 'local');
    }
}
