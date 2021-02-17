<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\Test\TestLogger;

class StorageApiWriterTest extends BaseWriterTest
{
    use CreateBranchTrait;

    const DEFAULT_SYSTEM_METADATA = ['componentId' => 'foo'];

    public function setUp()
    {
        parent::setUp();
        $this->clearFileUploads(['output-mapping-bundle-test']);
        $this->clearBuckets([
            'out.c-output-mapping-test',
            'out.c-output-mapping-default-test',
            'out.c-output-mapping-redshift-test',
            'in.c-output-mapping-test',
            'out.c-dev-123-output-mapping-test'
        ]);
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-redshift-test', 'out', '', 'redshift');
        $this->clientWrapper->getBasicClient()->createBucket('output-mapping-default-test', 'out');
    }

    public function testWriteBasicFiles()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents($root . "/upload/file2", "test");
        file_put_contents(
            $root . "/upload/file2.manifest",
            "{\"tags\": [\"output-mapping-test\", \"xxx\"],\"is_public\": false}"
        );
        file_put_contents($root . "/upload/file3", "test");
        file_put_contents($root . "/upload/file3.manifest", "{\"tags\": [\"output-mapping-test\"],\"is_public\": true}");

        $systemMetadata = [
            "componentId" => "testComponent",
            "configurationId" => "metadata-write-test",
            "configurationRowId" => "12345",
            "branchId" => "1234",
        ];

        $configs = [
            [
                "source" => "file1",
                "tags" => ["output-mapping-test"]
            ],
            [
                "source" => "file2",
                "tags" => ["output-mapping-test", "another-tag"],
                "is_public" => true
            ]
        ];

        $writer = new FileWriter($this->getStagingFactory());

        $writer->uploadFiles('/upload', ["mapping" => $configs], $systemMetadata, StrategyFactory::LOCAL);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(["output-mapping-test"]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        $this->assertCount(3, $files);

        $file1 = $file2 = $file3 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
            if ($file["name"] == 'file2') {
                $file2 = $file;
            }
            if ($file["name"] == 'file3') {
                $file3 = $file;
            }
        }

        $expectedTags = [
            'output-mapping-test',
            'componentId: testComponent',
            'configurationId: metadata-write-test',
            'configurationRowId: 12345',
            'branchId: 1234',
        ];
        $expectedFile2Tags = array_merge($expectedTags, ['another-tag']);

        $this->assertNotNull($file1);
        $this->assertNotNull($file2);
        $this->assertNotNull($file3);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals($expectedTags, $file1["tags"]);
        $this->assertEquals(sort($expectedFile2Tags), sort($file2["tags"]));
        $this->assertEquals($expectedTags, $file3["tags"]);
        $this->assertFalse($file1["isPublic"]);
        $this->assertTrue($file2["isPublic"]);
        $this->assertTrue($file3["isPublic"]);
    }

    public function testWriteFilesOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");

        $configs = [
            [
                "source" => "file1",
                "tags" => ["output-mapping-test"]
            ]
        ];

        $writer = new FileWriter($this->getStagingFactory());

        $writer->uploadFiles('/upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(["output-mapping-test"]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        $this->assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertEquals(4, $file1['sizeBytes']);
        $this->assertEquals(['output-mapping-test', 'componentId: foo'], $file1['tags']);
    }

    public function testWriteFilesOutputMappingDevMode()
    {
        $this->clearFileUploads(['dev-123-output-mapping-test']);
        $this->clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $branchId = $this->createBranch($this->clientWrapper, 'dev-123');
        $this->clientWrapper->setBranchId($branchId);

        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");

        $configs = [
            [
                "source" => "file1",
                "tags" => ["output-mapping-test"]
            ]
        ];

        $writer = new FileWriter($this->getStagingFactory());

        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => $branchId,
        ];

        $writer->uploadFiles('/upload', ['mapping' => $configs], $systemMetadata, StrategyFactory::LOCAL);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([sprintf('%s-output-mapping-test', $branchId)]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        $this->assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
        }

        $expectedTags = [
            sprintf('%s-output-mapping-test', $branchId),
            sprintf('%s-componentId: testComponent', $branchId),
            sprintf('%s-configurationId: metadata-write-test', $branchId),
            sprintf('%s-rowId: 12345', $branchId),
            sprintf('%s-branchId: %s', $branchId, $branchId),
        ];

        $this->assertNotNull($file1);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals($expectedTags, $file1['tags']);
    }

    public function testWriteFilesOutputMappingAndManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents(
            $root . "/upload/file1.manifest",
            "{\"tags\": [\"output-mapping-test\", \"xxx\"],\"is_public\": true}"
        );

        $configs = [
            [
                "source" => "file1",
                "tags" => ["output-mapping-test", "yyy"],
                "is_public" => false
            ]
        ];

        $writer = new FileWriter($this->getStagingFactory());
        $writer->uploadFiles('upload', ["mapping" => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(["output-mapping-test"]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        $this->assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals(["output-mapping-test", "yyy", "componentId: foo"], $file1["tags"]);
        $this->assertFalse($file1["isPublic"]);
    }

    public function testWriteFilesInvalidJson()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents($root . "/upload/file1.manifest", "this is not at all a {valid} json");

        $configs = [
            [
                "source" => "file1",
                "tags" => ["output-mapping-test", "yyy"],
                "is_public" => false
            ]
        ];

        $writer = new FileWriter($this->getStagingFactory());
        $writer->setFormat('json');
        try {
            $writer->uploadFiles('/upload', ['mapping' => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
            $this->fail('Invalid manifest must raise exception.');
        } catch (InvalidOutputException $e) {
            $this->assertContains('json', $e->getMessage());
        }
    }

    public function testWriteFilesInvalidYaml()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents($root . "/upload/file1.manifest", "\tthis is not \n\t \tat all a {valid} json");

        $configs = [
            [
                "source" => "file1",
                "tags" => ["output-mapping-test", "yyy"],
                "is_public" => false
            ]
        ];

        $writer = new FileWriter($this->getStagingFactory());
        $writer->setFormat('json');
        try {
            $writer->uploadFiles('upload', ['mapping' => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
            $this->fail('Invalid manifest must raise exception.');
        } catch (InvalidOutputException $e) {
            $this->assertContains('json', $e->getMessage());
        }
    }

    public function testWriteFilesOutputMappingMissing()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/file1", "test");
        file_put_contents(
            $root . "/upload/file1.manifest",
            "{\"tags\": [\"output-mapping-test-xxx\"],\"is_public\": true}"
        );

        $configs = [
            [
                "source" => "file2",
                "tags" => ["output-mapping-test"],
                "is_public" => false
            ]
        ];
        $writer = new FileWriter($this->getStagingFactory());
        try {
            $writer->uploadFiles('upload', ['mapping' => $configs], ['componentId' => 'foo'], StrategyFactory::LOCAL);
            $this->fail('Missing file must fail');
        } catch (InvalidOutputException $e) {
            $this->assertContains("File 'file2' not found", $e->getMessage());
            $this->assertEquals(404, $e->getCode());
        }
    }

    public function testWriteFilesOrphanedManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . "/upload/file1.manifest",
            "{\"tags\": [\"output-mapping-test-xxx\"],\"is_public\": true}"
        );
        $writer = new FileWriter($this->getStagingFactory());
        try {
            $writer->uploadFiles('/upload', [], ['componentId' => 'foo'], StrategyFactory::LOCAL);
            $this->fail('Orphaned manifest must cause exception.');
        } catch (InvalidOutputException $e) {
            $this->assertContains("Found orphaned file manifest: 'file1.manifest'", $e->getMessage());
        }
    }

    public function testWriteFilesNoComponentId()
    {
        $writer = new FileWriter($this->getStagingFactory());
        try {
            $writer->uploadFiles('/upload', [], ['componentId' => 'foo'], StrategyFactory::LOCAL);
            $this->fail('Missing componentId must cause exception.');
        } catch (InvalidOutputException $e) {
            $this->assertContains('Component Id must be set', $e->getMessage());
        }
    }

    public function testWriteTableOutputMapping()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table1a.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/upload/table2a.csv", "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n");

        $configs = [
            [
                "source" => "table1a.csv",
                "destination" => "out.c-output-mapping-test.table1a"
            ],
            [
                "source" => "table2a.csv",
                "destination" => "out.c-output-mapping-test.table2a"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);
    }

    public function testWriteTableOutputMappingDevMode()
    {
        $this->clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $branchId = $this->createBranch($this->clientWrapper, 'dev-123');
        $this->clientWrapper->setBranchId($branchId);

        $root = $this->tmp->getTmpFolder();
        $this->tmp->initRunFolder();
        file_put_contents($root . "/upload/table11a.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");
        file_put_contents($root . "/upload/table21a.csv", "\"Id2\",\"Name2\"\n\"test2\",\"test2\"\n\"aabb2\",\"ccdd2\"\n");

        $configs = [
            [
                "source" => "table11a.csv",
                "destination" => "out.c-output-mapping-test.table11a"
            ],
            [
                "source" => "table21a.csv",
                "destination" => "out.c-output-mapping-test.table21a"
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
        $tables = $this->clientWrapper->getBasicClient()->listTables(sprintf('out.c-%s-output-mapping-test', $branchId));
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(
            [
                sprintf('out.c-%s-output-mapping-test.table11a', $branchId),
                sprintf('out.c-%s-output-mapping-test.table21a', $branchId),
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
                "destination" => "out.c-output-mapping-test.table21"
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

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table21', $tables[0]["id"]);
        $this->assertNotEmpty($jobIds[0]);
    }

    public function testWriteTableOutputMappingWithoutCsv()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table31", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table31",
                "destination" => "out.c-output-mapping-test.table31"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table31', $tables[0]["id"]);
    }

    public function testWriteTableOutputMappingEmptyFile()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table41", "");

        $configs = [
            [
                "source" => "table41",
                "destination" => "out.c-output-mapping-test.table41"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        try {
            $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
            $this->fail("Empty CSV file must fail");
        } catch (InvalidOutputException $e) {
            $this->assertContains('no data in import file', $e->getMessage());
        }
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
            "{\"destination\": \"out.c-output-mapping-test.table2\",\"primary_key\": [\"Id\"]}"
        );

        $configs = [
            [
                "source" => "table2.csv",
                "destination" => "out.c-output-mapping-test.table"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table', $tables[0]["id"]);
        $this->assertEquals([], $tables[0]["primaryKey"]);
    }

    public function testWriteTableManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-test.table3a.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-test.table3a.csv.manifest",
            "{\"destination\": \"out.c-output-mapping-test.table3a\",\"primary_key\": [\"Id\",\"Name\"]}"
        );

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table3a', $tables[0]["id"]);
        $this->assertEquals(["Id", "Name"], $tables[0]["primaryKey"]);
    }

    public function testWriteTableInvalidManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-test.table3b.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-test.table3b.csv.manifest",
            "{\"destination\": \"out.c-output-mapping-test.table3\",\"primary_key\": \"Id\"}"
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
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-default-test.table3c.csv",
            "'Id'\t'Name'\n'test'\t'test''s'\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-default-test.table3c.csv.manifest",
            "{\"destination\": \"out.c-output-mapping-default-test.table3c\",\"delimiter\": \"\\t\",\"enclosure\": \"'\"}"
        );

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-default-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-default-test.table3c', $tables[0]["id"]);
        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-default-test.table3c', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertEquals(1, count($table));
        $this->assertEquals(2, count($table[0]));
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('test', $table[0]['Id']);
        $this->assertEquals('test\'s', $table[0]['Name']);
    }

    public function testWriteTableManifestCsvRedshift()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-redshift-test.table3d.csv",
            "'Id'\t'Name'\n'test'\t'test''s'\n"
        );
        file_put_contents(
            $root . DIRECTORY_SEPARATOR . "upload/out.c-output-mapping-redshift-test.table3d.csv.manifest",
            "{\"destination\": \"out.c-output-mapping-redshift-test.table3d\",\"delimiter\": \"\\t\",\"enclosure\": \"'\"}"
        );

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-redshift-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-redshift-test.table3d', $tables[0]["id"]);
        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-redshift-test.table3d', $downloadedFile, []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($downloadedFile));
        $this->assertEquals(1, count($table));
        $this->assertEquals(2, count($table[0]));
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
            "{\"destination\": \"out.c-output-mapping-test.table3e\",\"primary_key\": [\"Id\", \"Name\"]}"
        );
        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
            $this->fail("Orphaned manifest must fail");
        } catch (InvalidOutputException $e) {
            $this->assertContains("Found orphaned table manifest: 'table.csv.manifest'", $e->getMessage());
        }
    }

    public function testWriteTableOutputMappingMissing()
    {
        $root = $this->tmp->getTmpFolder();

        $configs = [
            [
                "source" => "table81.csv",
                "destination" => "out.c-output-mapping-test.table81"
            ]
        ];
        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
            $this->fail("Missing table file must fail");
        } catch (InvalidOutputException $e) {
            $this->assertContains("Table source 'table81.csv' not found", $e->getMessage());
            $this->assertEquals(404, $e->getCode());
        }
    }

    public function testWriteTableMetadataMissing()
    {
        $root = $this->tmp->getTmpFolder();

        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('/upload', [], [], 'local');
            self::fail("Missing metadata must fail.");
        } catch (OutputOperationException $e) {
            self::assertContains('Component Id must be set', $e->getMessage());
        }
    }

    public function testWriteTableBare()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-output-mapping-test.table4.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-output-mapping-test.table4', $tables[0]["id"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('out.c-output-mapping-test.table4');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);
    }

    public function testWriteTableBareWithoutSuffix()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-output-mapping-test.table4", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-output-mapping-test.table4', $tables[0]["id"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('out.c-output-mapping-test.table4');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);
    }

    public function testWriteTableIncrementalWithDeleteDefault()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table51.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table51.csv",
                "destination" => "out.c-output-mapping-default-test.table51",
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
        $exporter->exportTable("out.c-output-mapping-default-test.table51", $root . DIRECTORY_SEPARATOR . "download.csv", []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($root . DIRECTORY_SEPARATOR . "download.csv"));
        usort($table, function ($a, $b) {
            return strcasecmp($a['Id'], $b['Id']);
        });
        $this->assertEquals(3, count($table));
        $this->assertEquals(2, count($table[0]));
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('aabb', $table[0]['Id']);
        $this->assertEquals('ccdd', $table[0]['Name']);
        $this->assertEquals('test', $table[1]['Id']);
        $this->assertEquals('test', $table[1]['Name']);
        $this->assertEquals('test', $table[2]['Id']);
        $this->assertEquals('test', $table[2]['Name']);
    }

    public function testWriteTableIncrementalWithDeleteRedshift()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table61.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table61.csv",
                "destination" => "out.c-output-mapping-redshift-test.table61",
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

        // And again, check first incremental table
        $tableQueue =  $writer->uploadTables('/upload', ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $exporter = new TableExporter($this->clientWrapper->getBasicClient());
        $exporter->exportTable("out.c-output-mapping-redshift-test.table61", $root . DIRECTORY_SEPARATOR . "download.csv", []);
        $table = $this->clientWrapper->getBasicClient()->parseCsv(file_get_contents($root . DIRECTORY_SEPARATOR . "download.csv"));
        usort($table, function ($a, $b) {
            return strcasecmp($a['Id'], $b['Id']);
        });
        $this->assertEquals(3, count($table));
        $this->assertEquals(2, count($table[0]));
        $this->assertArrayHasKey('Id', $table[0]);
        $this->assertArrayHasKey('Name', $table[0]);
        $this->assertEquals('aabb', $table[0]['Id']);
        $this->assertEquals('ccdd', $table[0]['Name']);
        $this->assertEquals('test', $table[1]['Id']);
        $this->assertEquals('test', $table[1]['Name']);
        $this->assertEquals('test', $table[2]['Id']);
        $this->assertEquals('test', $table[2]['Name']);
    }

    public function testTagFiles()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/test", "test");

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload/test",
            (new FileUploadOptions())->setTags(["output-mapping-test"])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload/test",
            (new FileUploadOptions())->setTags(["output-mapping-test"])
        );
        sleep(1);

        $writer = new FileWriter($this->getStagingFactory());
        $configuration = [["tags" => ["output-mapping-test"], "processed_tags" => ['downloaded']]];
        $writer->tagFiles($configuration);

        $file = $this->clientWrapper->getBasicClient()->getFile($id1);
        $this->assertTrue(in_array('downloaded', $file['tags']));
        $file = $this->clientWrapper->getBasicClient()->getFile($id2);
        $this->assertTrue(in_array('downloaded', $file['tags']));
    }

    public function testWriteTableToDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table71.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table72.csv", "\"Id\",\"Name2\"\n\"test2\",\"test2\"\n");

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables('/upload', ["bucket" => "in.c-output-mapping-test"], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $this->assertEquals(2, $tableQueue->getTaskCount());

        $tables = $this->clientWrapper->getBasicClient()->listTables("in.c-output-mapping-test");
        $this->assertCount(2, $tables);

        $this->assertEquals('in.c-output-mapping-test.table71', $tables[0]["id"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('in.c-output-mapping-test.table71');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);

        $this->assertEquals('in.c-output-mapping-test.table72', $tables[1]["id"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('in.c-output-mapping-test.table72');
        $this->assertEquals(["Id", "Name2"], $tableInfo["columns"]);
    }

    public function testWriteTableBareWithDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table5.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['bucket' => 'out.c-output-mapping-test'],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertCount(1, $jobIds);

        $this->assertEquals('out.c-output-mapping-test.table5', $tables[0]["id"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('out.c-output-mapping-test.table5');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);
    }

    public function testWriteTableManifestWithDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . "/upload/table6.csv",
            "\"Id\",\"Name\"\n\"test\",\"test\"\n"
        );
        file_put_contents(
            $root . "/upload/table6.csv.manifest",
            "{\"primary_key\": [\"Id\", \"Name\"]}"
        );

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ['bucket' => 'out.c-output-mapping-test'],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table6', $tables[0]["id"]);
        $this->assertEquals(["Id", "Name"], $tables[0]["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table7.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                "source" => "table7.csv",
                "destination" => "table7"
            ]
        ];

        $writer = new TableWriter($this->getStagingFactory());

        $tableQueue =  $writer->uploadTables(
            '/upload',
            ["mapping" => $configs, 'bucket' => 'out.c-output-mapping-test'],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table7', $tables[0]["id"]);
    }

    public function testWriteManifestWithoutDestination()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table8.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table8.csv.manifest", "{\"primary_key\": [\"Id\", \"Name\"]}");

        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables('upload', ["mapping" => []], ['componentId' => 'foo'], 'local');
            $this->fail("Empty destination with invalid table name must cause exception.");
        } catch (InvalidOutputException $e) {
            $this->assertContains('valid table identifier', $e->getMessage());
        }
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
                        "destination" => "out.c-output-mapping-test.table16",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table16");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithPkOverwrite()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table15.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->getStagingFactory());
        $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table15.csv",
                        "destination" => "out.c-output-mapping-test.table15",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            '/upload',
            [
                "mapping" => [
                    [
                        "source" => "table15.csv",
                        "destination" => "out.c-output-mapping-test.table15",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tableInfo = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table15");
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
                        "destination" => "out.c-output-mapping-test.table14",
                        "primary_key" => ["Id"]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table14");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);

        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables(
                'upload',
                [
                    "mapping" => [
                        [
                            "source" => "table14.csv",
                            "destination" => "out.c-output-mapping-test.table14",
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
                'Output mapping does not match destination table: primary key "Id, Name" does not match "Id" in "out.c-output-mapping-test.table14".',
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
                        "destination" => "out.c-output-mapping-test.table13",
                        "primary_key" => ["Id "]
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table13");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);

        $writer = new TableWriter($this->getStagingFactory());
        try {
            $writer->uploadTables(
                'upload',
                [
                    "mapping" => [
                        [
                            "source" => "table13.csv",
                            "destination" => "out.c-output-mapping-test.table13",
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
                'Output mapping does not match destination table: primary key "Id, Name" does not match "Id" in "out.c-output-mapping-test.table13".',
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
        $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table12.csv",
                        "destination" => "out.c-output-mapping-test.table12",
                        "primary_key" => []
                    ]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table12");
        $this->assertEquals([], $tableInfo["primaryKey"]);

        $writer = new TableWriter($this->getStagingFactory(null, 'json', $testLogger));
        $tableQueue =  $writer->uploadTables(
            'upload',
            [
                "mapping" => [
                    [
                        "source" => "table12.csv",
                        "destination" => "out.c-output-mapping-test.table12",
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
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table12");
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
                        "destination" => "out.c-output-mapping-test.table11",
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
            '{"destination": "out.c-output-mapping-test.table11","primary_key": [""]}'
        );
        $tableQueue =  $writer->uploadTables('upload', [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->assertFalse(
            $testLogger->hasWarningThatContains(
                "Output mapping does not match destination table: primary key '' does not match '' in 'out.c-output-mapping-test.table9'."
            )
        );
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable("out.c-output-mapping-test.table11");
        $this->assertEquals([], $tableInfo["primaryKey"]);
    }

    public function testWriteTableColumnsOverwrite()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-output-mapping-test.table10.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue = $writer->uploadTables('upload', [], ['componentId' => 'foo'], 'local');
        $tableQueue->waitForAll();

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-output-mapping-test.table10', $tables[0]["id"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('out.c-output-mapping-test.table10');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);

        file_put_contents($root . "/upload/out.c-output-mapping-test.table10.csv", "\"foo\",\"bar\"\n\"baz\",\"bat\"\n");
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ["mapping" => [["source" => "out.c-output-mapping-test.table10.csv", "columns" => ["Boing", "Tschak"]]]],
            ['componentId' => 'foo'],
            'local'
        );
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessageRegExp('/Some columns are missing in the csv file. Missing columns: id,name./i');
        $tableQueue->waitForAll();
    }

    public function testWriteMultipleErrors()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-output-mapping-test.table10a.csv", "\"id\",\"name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/out.c-output-mapping-test.table10b.csv", "\"foo\",\"bar\"\n\"baz\",\"bat\"\n");
        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ["mapping" => [["source" => "out.c-output-mapping-test.table10a.csv"], ["source" => "out.c-output-mapping-test.table10b.csv"]]],
            ['componentId' => 'foo'],
            'local'
        );
        $tableQueue->waitForAll();

        $tables = $this->clientWrapper->getBasicClient()->listTables("out.c-output-mapping-test");
        $this->assertCount(2, $tables);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('out.c-output-mapping-test.table10a');
        $this->assertEquals(["id", "name"], $tableInfo["columns"]);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('out.c-output-mapping-test.table10b');
        $this->assertEquals(["foo", "bar"], $tableInfo["columns"]);

        $writer = new TableWriter($this->getStagingFactory());
        $tableQueue =  $writer->uploadTables(
            'upload',
            ["mapping" =>
                [
                    ["source" => "out.c-output-mapping-test.table10a.csv", "columns" => ["Boing", "Tschak"]],
                    ["source" => "out.c-output-mapping-test.table10b.csv", "columns" => ["bum", "tschak"]]
                ]
            ],
            ['componentId' => 'foo'],
            'local'
        );
        try {
            $tableQueue->waitForAll();
            $this->fail("Must raise exception");
        } catch (InvalidOutputException $e) {
            $this->assertContains(
                'Failed to load table "out.c-output-mapping-test.table10a": Some columns are ' .
                'missing in the csv file. Missing columns: id,name. Expected columns: id,name. Please check if the ' .
                'expected delimiter "," is used in the csv file.',
                $e->getMessage()
            );
            $this->assertContains(
                'Failed to load table "out.c-output-mapping-test.table10b": Some columns are ' .
                'missing in the csv file. Missing columns: foo,bar. Expected columns: foo,bar. Please check if the ' .
                'expected delimiter "," is used in the csv file.',
                $e->getMessage()
            );
        }
    }

    public function testWriteTableInvalidCsv()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-output-mapping-test.table10.csv", "\"Id\",\"Name\"\r\"test\",\"test\"\r");
        $writer = new TableWriter($this->getStagingFactory());
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('Invalid line break. Please use unix \n or win \r\n line breaks.');
        $writer->uploadTables('/upload', [], ['componentId' => 'foo'], 'local');
    }

    public function testWriteTableExistingBucketDevModeNoDev()
    {
        $root = $this->tmp->getTmpFolder();
        $this->clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $branchId = $this->createBranch($this->clientWrapper, 'dev-123');
        $this->clientWrapper->setBranchId($branchId);

        file_put_contents($root . '/upload/table21.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table21.csv',
                'destination' => 'out.c-output-mapping-test.table21'
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
        $bucketId = sprintf('out.c-%s-output-mapping-test', $branchId);
        foreach ($metadata->listBucketMetadata($bucketId) as $metadatum) {
            if (($metadatum['key'] === 'KBC.createdBy.branch.id') || ($metadatum['key'] === 'KBC.lastUpdatedBy.branch.id')) {
                $metadata->deleteBucketMetadata($bucketId, $metadatum['id']);
            }
        }
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage(sprintf(
            'Trying to create a table in the development bucket ' .
            '"out.c-%s-output-mapping-test" on branch "dev-123" (ID "%s"), but the bucket is not assigned ' .
            'to any development branch.',
            $branchId,
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
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN_MASTER,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $branchId = $this->createBranch($this->clientWrapper, 'dev-123');
        $this->clientWrapper->setBranchId($branchId);

        file_put_contents($root . '/upload/table21.csv', "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n");

        $configs = [
            [
                'source' => 'table21.csv',
                'destination' => 'out.c-output-mapping-test.table21'
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
        $bucketId = sprintf('out.c-%s-output-mapping-test', $branchId);
        foreach ($metadata->listBucketMetadata($bucketId) as $metadatum) {
            if (($metadatum['key'] === 'KBC.createdBy.branch.id') || ($metadatum['key'] === 'KBC.lastUpdatedBy.branch.id')) {
                $metadata->deleteBucketMetadata($bucketId, $metadatum['id']);
                $metadata->postBucketMetadata(
                    $bucketId,
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

        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage(sprintf(
            'Trying to create a table in the development bucket ' .
            '"out.c-%s-output-mapping-test" on branch "dev-123" (ID "%s"). ' .
            'The bucket metadata marks it as assigned to branch with ID "12345".',
            $branchId,
            $branchId
        ));
        $writer->uploadTables(
            '/upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            StrategyFactory::LOCAL
        );
    }
}
