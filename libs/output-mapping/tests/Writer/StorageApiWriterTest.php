<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Log\NullLogger;

class StorageApiWriterTest extends BaseWriterTest
{
    public function setUp()
    {
        parent::setUp();
        $this->clearFileUploads(['output-mapping-bundle-test']);
        $this->clearBuckets([
            'out.c-output-mapping-test',
            'out.c-output-mapping-default-test',
            'out.c-output-mapping-redshift-test',
            'in.c-output-mapping-test',
        ]);
        $this->client->createBucket('output-mapping-redshift-test', 'out', '', 'redshift');
        $this->client->createBucket('output-mapping-default-test', 'out');
    }

    public function testWriteFiles()
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

        $writer = new FileWriter($this->client, new NullLogger());

        $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(["output-mapping-test"]);
        $files = $this->client->listFiles($options);
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

        $this->assertNotNull($file1);
        $this->assertNotNull($file2);
        $this->assertNotNull($file3);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals(["output-mapping-test"], $file1["tags"]);
        $this->assertEquals(["output-mapping-test", "another-tag"], $file2["tags"]);
        $this->assertEquals(["output-mapping-test"], $file3["tags"]);
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

        $writer = new FileWriter($this->client, new NullLogger());

        $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(["output-mapping-test"]);
        $files = $this->client->listFiles($options);
        $this->assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals(["output-mapping-test"], $file1["tags"]);
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

        $writer = new FileWriter($this->client, new NullLogger());
        $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(["output-mapping-test"]);
        $files = $this->client->listFiles($options);
        $this->assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file["name"] == 'file1') {
                $file1 = $file;
            }
        }

        $this->assertNotNull($file1);
        $this->assertEquals(4, $file1["sizeBytes"]);
        $this->assertEquals(["output-mapping-test", "yyy"], $file1["tags"]);
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

        $writer = new FileWriter($this->client, new NullLogger());
        $writer->setFormat('json');
        try {
            $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
            $this->fail("Invalid manifest must raise exception.");
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

        $writer = new FileWriter($this->client, new NullLogger());
        $writer->setFormat('json');
        try {
            $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
            $this->fail("Invalid manifest must raise exception.");
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
        $writer = new FileWriter($this->client, new NullLogger());
        try {
            $writer->uploadFiles($root . "/upload", ["mapping" => $configs]);
            $this->fail("Missing file must fail");
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
        $writer = new FileWriter($this->client, new NullLogger());
        try {
            $writer->uploadFiles($root . "/upload");
            $this->fail("Orphaned manifest must cause exception.");
        } catch (InvalidOutputException $e) {
            $this->assertContains("Found orphaned file manifest: 'file1.manifest'", $e->getMessage());
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]["id"], $tables[1]["id"]];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        // And again
        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        try {
            $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        try {
            $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-default-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-default-test.table3c', $tables[0]["id"]);
        $exporter = new TableExporter($this->client);
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-default-test.table3c', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-redshift-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-redshift-test.table3d', $tables[0]["id"]);
        $exporter = new TableExporter($this->client);
        $downloadedFile = $root . DIRECTORY_SEPARATOR . "download.csv";
        $exporter->exportTable('out.c-output-mapping-redshift-test.table3d', $downloadedFile, []);
        $table = $this->client->parseCsv(file_get_contents($downloadedFile));
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
        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        try {
            $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
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
        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        try {
            $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
            $this->fail("Missing table file must fail");
        } catch (InvalidOutputException $e) {
            $this->assertContains("Table source 'table81.csv' not found", $e->getMessage());
            $this->assertEquals(404, $e->getCode());
        }
    }

    public function testWriteTableMetadataMissing()
    {
        $root = $this->tmp->getTmpFolder();

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        try {
            $writer->uploadTables($root . "/upload", [], [], 'local');
            self::fail("Missing metadata must fail.");
        } catch (OutputOperationException $e) {
            self::assertContains('Component Id must be set', $e->getMessage());
        }
    }

    public function testWriteTableBare()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-output-mapping-test.table4.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-output-mapping-test.table4', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('out.c-output-mapping-test.table4');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);
    }

    public function testWriteTableBareWithoutSuffix()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-output-mapping-test.table4", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-output-mapping-test.table4', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('out.c-output-mapping-test.table4');
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->client->handleAsyncTasks($jobIds);

        // And again, check first incremental table
        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->client->handleAsyncTasks($jobIds);

        $exporter = new TableExporter($this->client);
        $exporter->exportTable("out.c-output-mapping-default-test.table51", $root . DIRECTORY_SEPARATOR . "download.csv", []);
        $table = $this->client->parseCsv(file_get_contents($root . DIRECTORY_SEPARATOR . "download.csv"));
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        // And again, check first incremental table
        $tableQueue =  $writer->uploadTables($root . "/upload", ["mapping" => $configs], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $exporter = new TableExporter($this->client);
        $exporter->exportTable("out.c-output-mapping-redshift-test.table61", $root . DIRECTORY_SEPARATOR . "download.csv", []);
        $table = $this->client->parseCsv(file_get_contents($root . DIRECTORY_SEPARATOR . "download.csv"));
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

        $id1 = $this->client->uploadFile(
            $root . "/upload/test",
            (new FileUploadOptions())->setTags(["output-mapping-test"])
        );
        $id2 = $this->client->uploadFile(
            $root . "/upload/test",
            (new FileUploadOptions())->setTags(["output-mapping-test"])
        );
        sleep(1);

        $writer = new FileWriter($this->client, new NullLogger());
        $configuration = [["tags" => ["output-mapping-test"], "processed_tags" => ['downloaded']]];
        $writer->tagFiles($configuration);

        $file = $this->client->getFile($id1);
        $this->assertTrue(in_array('downloaded', $file['tags']));
        $file = $this->client->getFile($id2);
        $this->assertTrue(in_array('downloaded', $file['tags']));
    }

    public function testWriteTableToDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table71.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table72.csv", "\"Id\",\"Name2\"\n\"test2\",\"test2\"\n");

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables($root . "/upload", ["bucket" => "in.c-output-mapping-test"], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);
        $this->assertEquals(2, $tableQueue->getTaskCount());

        $tables = $this->client->listTables("in.c-output-mapping-test");
        $this->assertCount(2, $tables);

        $this->assertEquals('in.c-output-mapping-test.table71', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('in.c-output-mapping-test.table71');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);

        $this->assertEquals('in.c-output-mapping-test.table72', $tables[1]["id"]);
        $tableInfo = $this->client->getTable('in.c-output-mapping-test.table72');
        $this->assertEquals(["Id", "Name2"], $tableInfo["columns"]);
    }

    public function testWriteTableBareWithDefaultBucket()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table5.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
            ['bucket' => 'out.c-output-mapping-test'],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertCount(1, $jobIds);

        $this->assertEquals('out.c-output-mapping-test.table5', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('out.c-output-mapping-test.table5');
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
            ['bucket' => 'out.c-output-mapping-test'],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());

        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
            ["mapping" => $configs, 'bucket' => 'out.c-output-mapping-test'],
            ['componentId' => 'foo'],
            'local'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);
        $this->assertEquals('out.c-output-mapping-test.table7', $tables[0]["id"]);
    }

    public function testWriteManifestWithoutDestination()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table8.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        file_put_contents($root . "/upload/table8.csv.manifest", "{\"primary_key\": [\"Id\", \"Name\"]}");

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        try {
            $writer->uploadTables($root . "/upload", ["mapping" => []], ['componentId' => 'foo'], 'local');
            $this->fail("Empty destination with invalid table name must cause exception.");
        } catch (InvalidOutputException $e) {
            $this->assertContains('valid table identifier', $e->getMessage());
        }
    }

    public function testWriteTableOutputMappingWithPk()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table16.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
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
        $tableInfo = $this->client->getTable("out.c-output-mapping-test.table16");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithPkOverwrite()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table15.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $writer->uploadTables(
            $root . "/upload",
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

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
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

        $tableInfo = $this->client->getTable("out.c-output-mapping-test.table15");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithPkMismatch()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table14.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
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
        $tableInfo = $this->client->getTable("out.c-output-mapping-test.table14");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        try {
            $writer->uploadTables(
                $root . "/upload",
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
        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
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
        $tableInfo = $this->client->getTable("out.c-output-mapping-test.table13");
        $this->assertEquals(["Id"], $tableInfo["primaryKey"]);

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        try {
            $writer->uploadTables(
                $root . "/upload",
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

        $handler = new TestHandler();

        $writer = new TableWriter($this->client, (new Logger("null"))->pushHandler($handler), new NullWorkspaceProvider());
        $writer->uploadTables(
            $root . "/upload",
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
        $tableInfo = $this->client->getTable("out.c-output-mapping-test.table12");
        $this->assertEquals([], $tableInfo["primaryKey"]);


        $writer = new TableWriter($this->client, (new Logger("null"))->pushHandler($handler), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
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
        $this->client->handleAsyncTasks($jobIds);
        $this->assertFalse($handler->hasWarningThatContains("Output mapping does not match destination table"));
        $tableInfo = $this->client->getTable("out.c-output-mapping-test.table12");
        $this->assertEquals([], $tableInfo["primaryKey"]);
    }

    public function testWriteTableOutputMappingWithEmptyStringPkInManifest()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/table11.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");

        $handler = new TestHandler();

        $writer = new TableWriter($this->client, (new Logger("null"))->pushHandler($handler), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
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

        $writer = new TableWriter($this->client, (new Logger("null"))->pushHandler($handler), new NullWorkspaceProvider());
        file_put_contents(
            $root . "/upload/table11.csv.manifest",
            '{"destination": "out.c-output-mapping-test.table11","primary_key": [""]}'
        );
        $tableQueue =  $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(1, $jobIds);
        $this->assertFalse(
            $handler->hasWarningThatContains(
                "Output mapping does not match destination table: primary key '' does not match '' in 'out.c-output-mapping-test.table9'."
            )
        );
        $tableInfo = $this->client->getTable("out.c-output-mapping-test.table11");
        $this->assertEquals([], $tableInfo["primaryKey"]);
    }

    public function testWriteTableColumnsOverwrite()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/out.c-output-mapping-test.table10.csv", "\"Id\",\"Name\"\n\"test\",\"test\"\n");
        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue = $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
        $tableQueue->waitForAll();

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(1, $tables);

        $this->assertEquals('out.c-output-mapping-test.table10', $tables[0]["id"]);
        $tableInfo = $this->client->getTable('out.c-output-mapping-test.table10');
        $this->assertEquals(["Id", "Name"], $tableInfo["columns"]);

        file_put_contents($root . "/upload/out.c-output-mapping-test.table10.csv", "\"foo\",\"bar\"\n\"baz\",\"bat\"\n");
        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
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
        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
            ["mapping" => [["source" => "out.c-output-mapping-test.table10a.csv"], ["source" => "out.c-output-mapping-test.table10b.csv"]]],
            ['componentId' => 'foo'],
            'local'
        );
        $tableQueue->waitForAll();

        $tables = $this->client->listTables("out.c-output-mapping-test");
        $this->assertCount(2, $tables);
        $tableInfo = $this->client->getTable('out.c-output-mapping-test.table10a');
        $this->assertEquals(["id", "name"], $tableInfo["columns"]);
        $tableInfo = $this->client->getTable('out.c-output-mapping-test.table10b');
        $this->assertEquals(["foo", "bar"], $tableInfo["columns"]);

        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        $tableQueue =  $writer->uploadTables(
            $root . "/upload",
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
        $writer = new TableWriter($this->client, new NullLogger(), new NullWorkspaceProvider());
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('Invalid line break. Please use unix \n or win \r\n line breaks.');
        $writer->uploadTables($root . "/upload", [], ['componentId' => 'foo'], 'local');
    }
}
