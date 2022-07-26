<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Writer\BaseWriterTest;
use Keboola\OutputMapping\Tests\Writer\CreateBranchTrait;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class StorageApiFileWriterTest extends BaseWriterTest
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
            "runId" => "999",
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
            'runId: 999',
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

        $writer->uploadFiles('/upload', ["mapping" => $configs], [], StrategyFactory::LOCAL);
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
        $this->assertEquals(['output-mapping-test'], $file1['tags']);
    }

    public function testWriteFilesOutputMappingDevMode()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                STORAGE_API_URL,
                STORAGE_API_TOKEN_MASTER,
                null
            )
        );
        $branchId = $this->createBranch($clientWrapper, 'dev-123');
        $this->clearFileUploads(['dev-123-output-mapping-test']);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                STORAGE_API_URL,
                STORAGE_API_TOKEN,
                $branchId
            )
        );

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
            'runId' => '999',
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
            sprintf('%s-configurationRowId: 12345', $branchId),
            sprintf('%s-branchId: %s', $branchId, $branchId),
            sprintf('%s-runId: 999', $branchId),
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
            $writer->uploadFiles('/upload', ['mapping' => $configs], [], StrategyFactory::LOCAL);
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
            $writer->uploadFiles('upload', ['mapping' => $configs], [], StrategyFactory::LOCAL);
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
            $writer->uploadFiles('upload', ['mapping' => $configs], [], StrategyFactory::LOCAL);
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
            $writer->uploadFiles('/upload', [], [], StrategyFactory::LOCAL);
            $this->fail('Orphaned manifest must cause exception.');
        } catch (InvalidOutputException $e) {
            $this->assertContains("Found orphaned file manifest: 'file1.manifest'", $e->getMessage());
        }
    }

    public function testWriteFilesNoComponentId()
    {
        $writer = new FileWriter($this->getStagingFactory());
        try {
            $writer->uploadFiles('/upload', [], ['configurationId' => '123'], StrategyFactory::LOCAL);
            $this->fail('Missing componentId must cause exception.');
        } catch (OutputOperationException $e) {
            $this->assertContains('Component Id must be set', $e->getMessage());
        }
    }

    public function testTagProcessedFiles()
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

    public function testTagBranchProcessedFiles()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . "/upload/test", "test");

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload/test",
            (new FileUploadOptions())->setTags(["output-mapping-test"])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload/test",
            (new FileUploadOptions())->setTags(["12345-output-mapping-test"])
        );
        sleep(1);
        // set it to use a branch
        $this->initClient('12345');

        $writer = new FileWriter($this->getStagingFactory());
        $configuration = [["tags" => ["output-mapping-test"], "processed_tags" => ['downloaded']]];
        $writer->tagFiles($configuration);

        // first file shouldn't be marked as processed because a branch file exists
        $file1 = $this->clientWrapper->getBasicClient()->getFile($id1);
        $this->assertTrue(!in_array('12345-downloaded', $file1['tags']));
        $file2 = $this->clientWrapper->getBasicClient()->getFile($id2);
        $this->assertTrue(in_array('12345-downloaded', $file2['tags']));
        $this->assertTrue(in_array('12345-output-mapping-test', $file2['tags']));
    }

    public function testTableFiles()
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/file', 'test');
        // in this case there may be a table manifest present
        file_put_contents(
            $root . '/upload/file.manifest',
            '{"primary_key": ["Id", "Name"]}'
        );
        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '',
            'runId' => '999',
        ];
        $tableFiles = [
            'tags' => ['output-mapping-test', 'another-tag'],
            'is_permanent' => true,
        ];

        $writer = new FileWriter($this->getStagingFactory());

        $writer->uploadFiles(
            '/upload',
            [],
            $systemMetadata,
            StrategyFactory::LOCAL,
            $tableFiles
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags(["output-mapping-test"]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        $this->assertCount(1, $files);

        $expectedTags = [
            'output-mapping-test',
            'another-tag',
            'componentId: testComponent',
            'configurationId: metadata-write-test',
            'configurationRowId: 12345',
            'branchId: ',
            'runId: 999',
        ];
        $file = $files[0];
        $this->assertNotNull($file);
        $this->assertEquals(4, $file['sizeBytes']);
        $this->assertEquals(sort($expectedTags), sort($file['tags']));
        $this->assertNull($file['maxAgeDays']);
    }
}
