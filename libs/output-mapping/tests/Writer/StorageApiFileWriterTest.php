<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class StorageApiFileWriterTest extends AbstractTestCase
{
    private string $testFileTag;

    public function setUp(): void
    {
        parent::setUp();

        $this->testFileTag = $this->getFileTag();

        $this->clearFileUploads([$this->testFileTag]);
    }

    public function testWriteBasicFiles(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');
        file_put_contents($root . '/upload/file2', 'test');
        file_put_contents(
            $root . '/upload/file2.manifest',
            '{"tags": ["' . $this->testFileTag . '", "xxx"],"is_public": false}',
        );
        file_put_contents($root . '/upload/file3', 'test');
        file_put_contents(
            $root . '/upload/file3.manifest',
            '{"tags": ["' . $this->testFileTag . '"],"is_public": true}',
        );

        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '1234',
            'runId' => '999',
        ];

        $configs = [
            [
                'source' => 'file1',
                'tags' => [$this->testFileTag],
            ],
            [
                'source' => 'file2',
                'tags' => [$this->testFileTag, 'another-tag'],
                'is_public' => true,
            ],
        ];

        $writer = new FileWriter($this->getLocalStagingFactory());

        $writer->uploadFiles(
            '/upload',
            ['mapping' => $configs],
            $systemMetadata,
            AbstractStrategyFactory::LOCAL,
            [],
            false,
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([$this->testFileTag]);
        $files = $this->clientWrapper->getTableAndFileStorageClient()->listFiles($options);
        self::assertCount(3, $files);

        $file1 = $file2 = $file3 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
            if ($file['name'] === 'file2') {
                $file2 = $file;
            }
            if ($file['name'] === 'file3') {
                $file3 = $file;
            }
        }

        $expectedTags = [
            $this->testFileTag,
            'componentId: testComponent',
            'configurationId: metadata-write-test',
            'configurationRowId: 12345',
            'branchId: 1234',
            'runId: 999',
        ];
        $expectedFile2Tags = array_merge($expectedTags, ['another-tag']);

        self::assertNotNull($file1);
        self::assertNotNull($file2);
        self::assertNotNull($file3);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals($expectedTags, $file1['tags']);
        self::assertEquals(sort($expectedFile2Tags), sort($file2['tags']));
        self::assertEquals($expectedTags, $file3['tags']);
        self::assertFalse($file1['isPublic']);
        self::assertTrue($file2['isPublic']);
        self::assertTrue($file3['isPublic']);
    }

    public function testWriteFilesFailedJob(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');

        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '1234',
            'runId' => '999',
        ];

        $configs = [
            [
                'source' => 'file1',
                'tags' => [$this->testFileTag],
            ],
        ];

        $writer = new FileWriter($this->getLocalStagingFactory());

        $writer->uploadFiles(
            '/upload',
            ['mapping' => $configs],
            $systemMetadata,
            AbstractStrategyFactory::LOCAL,
            [],
            true,
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([$this->testFileTag]);
        $files = $this->clientWrapper->getTableAndFileStorageClient()->listFiles($options);
        // no files should be uploaded since, isFailedJob was true and write_always is not implemented for files
        self::assertCount(0, $files);
    }

    public function testWriteFilesOutputMapping(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');

        $configs = [
            [
                'source' => 'file1',
                'tags' => [$this->testFileTag],
            ],
        ];

        $writer = new FileWriter($this->getLocalStagingFactory());

        $writer->uploadFiles(
            '/upload',
            ['mapping' => $configs],
            [],
            AbstractStrategyFactory::LOCAL,
            [],
            false,
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([$this->testFileTag]);
        $files = $this->clientWrapper->getTableAndFileStorageClient()->listFiles($options);
        self::assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
        }

        self::assertNotNull($file1);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals([$this->testFileTag], $file1['tags']);
    }

    #[NeedsDevBranch]
    public function testWriteFilesOutputMappingFakeDevMode(): void
    {
        $this->clearFileUploads([$this->devBranchName . '-' . $this->testFileTag]);

        $this->initClient($this->devBranchId);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');

        $configs = [
            [
                'source' => 'file1',
                'tags' => [$this->testFileTag],
            ],
        ];

        $writer = new FileWriter($this->getLocalStagingFactory());

        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => $this->devBranchId,
            'runId' => '999',
        ];

        $writer->uploadFiles(
            '/upload',
            ['mapping' => $configs],
            $systemMetadata,
            AbstractStrategyFactory::LOCAL,
            [],
            false,
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([sprintf('%s-' . $this->testFileTag, $this->devBranchId)]);
        $files = $this->clientWrapper->getTableAndFileStorageClient()->listFiles($options);
        self::assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
        }

        $expectedTags = [
            sprintf('%s-' . $this->testFileTag, $this->devBranchId),
            sprintf('%s-componentId: testComponent', $this->devBranchId),
            sprintf('%s-configurationId: metadata-write-test', $this->devBranchId),
            sprintf('%s-configurationRowId: 12345', $this->devBranchId),
            sprintf('%s-branchId: %s', $this->devBranchId, $this->devBranchId),
            sprintf('%s-runId: 999', $this->devBranchId),
        ];

        self::assertNotNull($file1);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals($expectedTags, $file1['tags']);
    }

    #[NeedsDevBranch]
    public function testWriteFilesOutputMappingRealDevMode(): void
    {
        $this->clearFileUploads([$this->devBranchName . '-' . $this->testFileTag]);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN'),
                branchId: $this->devBranchId,
                useBranchStorage: true, // This is the important setting
            ),
        );

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');

        $configs = [
            [
                'source' => 'file1',
                'tags' => [$this->testFileTag],
            ],
        ];

        // pass the special client wrapper to the factory
        $writer = new FileWriter($this->getLocalStagingFactory($this->clientWrapper));

        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => $this->devBranchId,
            'runId' => '999',
        ];

        $writer->uploadFiles(
            '/upload',
            ['mapping' => $configs],
            $systemMetadata,
            AbstractStrategyFactory::LOCAL,
            [],
            false,
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([$this->testFileTag]);
        $files = $this->clientWrapper->getBranchClient()->listFiles($options);
        self::assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
        }

        $expectedTags = [
            $this->testFileTag,
            'componentId: testComponent',
            'configurationId: metadata-write-test',
            'configurationRowId: 12345',
            sprintf('branchId: %s', $this->devBranchId),
            'runId: 999',
        ];

        self::assertNotNull($file1);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals($expectedTags, $file1['tags']);
    }

    public function testWriteFilesOutputMappingAndManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');
        file_put_contents(
            $root . '/upload/file1.manifest',
            '{"tags": ["' . $this->testFileTag . '", "xxx"],"is_public": true}',
        );

        $configs = [
            [
                'source' => 'file1',
                'tags' => [$this->testFileTag, 'yyy'],
                'is_public' => false,
            ],
        ];

        $writer = new FileWriter($this->getLocalStagingFactory());
        $writer->uploadFiles(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL,
            [],
            false,
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([$this->testFileTag]);
        $files = $this->clientWrapper->getTableAndFileStorageClient()->listFiles($options);
        self::assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
        }

        self::assertNotNull($file1);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals([$this->testFileTag, 'yyy', 'componentId: foo'], $file1['tags']);
        self::assertFalse($file1['isPublic']);
    }

    public function testWriteFilesOutputMappingMissing(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');
        file_put_contents(
            $root . '/upload/file1.manifest',
            '{"tags": ["' . $this->testFileTag . '-xxx"],"is_public": true}',
        );

        $configs = [
            [
                'source' => 'file2',
                'tags' => [$this->testFileTag],
                'is_public' => false,
            ],
        ];
        $writer = new FileWriter($this->getLocalStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage("File 'file2' not found");
        $this->expectExceptionCode(404);
        $writer->uploadFiles(
            'upload',
            ['mapping' => $configs],
            [],
            AbstractStrategyFactory::LOCAL,
            [],
            false,
        );
    }

    public function testWriteFilesOrphanedManifest(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/file1.manifest',
            '{"tags": ["' . $this->testFileTag . '-xxx"],"is_public": true}',
        );
        $writer = new FileWriter($this->getLocalStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage("Found orphaned file manifest: 'file1.manifest'");
        $writer->uploadFiles(
            '/upload',
            [],
            [],
            AbstractStrategyFactory::LOCAL,
            [],
            false,
        );
    }

    public function testWriteFilesNoComponentId(): void
    {
        $writer = new FileWriter($this->getLocalStagingFactory());
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');
        $writer->uploadFiles(
            '/upload',
            [],
            ['configurationId' => '123'],
            AbstractStrategyFactory::LOCAL,
            [],
            false,
        );
    }

    public function testTagProcessedFiles(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/test', 'test');

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        sleep(1);

        $writer = new FileWriter($this->getLocalStagingFactory());
        $configuration = [['tags' => [$this->testFileTag], 'processed_tags' => ['downloaded']]];
        $writer->tagFiles($configuration);

        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($id1);
        self::assertTrue(in_array('downloaded', $file['tags']));
        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($id2);
        self::assertTrue(in_array('downloaded', $file['tags']));
    }

    public function testTagProcessedFilesIsIgnoredWhenBranchStorageFlagIsUsed(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/test', 'test');

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        sleep(1);

        $writer = new FileWriter($this->getLocalStagingFactory(new ClientWrapper(
            $this->clientWrapper->getClientOptionsReadOnly()->setUseBranchStorage(true),
        )));
        $configuration = [['tags' => [$this->testFileTag], 'processed_tags' => ['downloaded']]];
        $writer->tagFiles($configuration);

        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($id1);
        self::assertSame([$this->testFileTag], $file['tags']);
        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($id2);
        self::assertSame([$this->testFileTag], $file['tags']);
    }

    #[NeedsDevBranch]
    public function testTagBranchProcessedFiles(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/test', 'test');

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([$this->devBranchId . '-' . $this->testFileTag]),
        );
        sleep(1);
        // set it to use a branch
        $this->initClient($this->devBranchId);

        $writer = new FileWriter($this->getLocalStagingFactory());
        $configuration = [['tags' => [$this->testFileTag], 'processed_tags' => ['downloaded']]];
        $writer->tagFiles($configuration);

        // first file shouldn't be marked as processed because a branch file exists
        $file1 = $this->clientWrapper->getTableAndFileStorageClient()->getFile($id1);
        self::assertTrue(!in_array($this->devBranchId . '-downloaded', $file1['tags']));
        $file2 = $this->clientWrapper->getTableAndFileStorageClient()->getFile($id2);
        self::assertTrue(in_array($this->devBranchId . '-downloaded', $file2['tags']));
        self::assertTrue(in_array($this->devBranchId . '-' . $this->testFileTag, $file2['tags']));
    }

    #[NeedsDevBranch]
    public function testTagBranchProcessedFilesIsIgnoredWhenBranchStorageFlagIsUsed(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/test', 'test');

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([$this->devBranchId . '-' . $this->testFileTag]),
        );
        sleep(1);
        // set it to use a branch
        $this->initClient($this->devBranchId);

        $writer = new FileWriter($this->getLocalStagingFactory(new ClientWrapper(
            $this->clientWrapper->getClientOptionsReadOnly()->setUseBranchStorage(true),
        )));
        $configuration = [['tags' => [$this->testFileTag], 'processed_tags' => ['downloaded']]];
        $writer->tagFiles($configuration);

        // first file shouldn't be marked as processed because a branch file exists
        $file1 = $this->clientWrapper->getTableAndFileStorageClient()->getFile($id1);
        self::assertSame([$this->testFileTag], $file1['tags']);
        $file2 = $this->clientWrapper->getTableAndFileStorageClient()->getFile($id2);
        self::assertSame([$this->devBranchId . '-' . $this->testFileTag], $file2['tags']);
    }

    public function testTableFiles(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/file', 'test');
        // in this case there may be a table manifest present
        file_put_contents(
            $root . '/upload/file.manifest',
            '{"primary_key": ["Id", "Name"]}',
        );
        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => '',
            'runId' => '999',
        ];
        $tableFiles = [
            'tags' => [$this->testFileTag, 'another-tag'],
            'is_permanent' => true,
        ];

        $writer = new FileWriter($this->getLocalStagingFactory());

        $writer->uploadFiles(
            '/upload',
            [],
            $systemMetadata,
            AbstractStrategyFactory::LOCAL,
            $tableFiles,
            false,
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([$this->testFileTag]);
        $files = $this->clientWrapper->getTableAndFileStorageClient()->listFiles($options);
        self::assertCount(1, $files);

        $expectedTags = [
            $this->testFileTag,
            'another-tag',
            'componentId: testComponent',
            'configurationId: metadata-write-test',
            'configurationRowId: 12345',
            'branchId: ',
            'runId: 999',
        ];
        $file = $files[0];
        self::assertNotNull($file);
        self::assertEquals(4, $file['sizeBytes']);
        self::assertEquals(sort($expectedTags), sort($file['tags']));
        self::assertNull($file['maxAgeDays']);
    }

    private function getFileTag(): string
    {
        $tag = $this->getName(false);
        $dataName = (string) $this->dataName();

        if ($dataName) {
            $tag .= '-' . $dataName;
        }

        return $tag;
    }
}
