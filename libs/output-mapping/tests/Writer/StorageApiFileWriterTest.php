<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\FileWriter;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class StorageApiFileWriterTest extends BaseWriterTest
{
    use CreateBranchTrait;

    private const FILE_TAG = 'StorageApiFileWriterTest';
    private const OUTPUT_BUCKET = 'out.c-StorageApiFileWriterTest';

    public function setUp(): void
    {
        parent::setUp();
        $this->clearFileUploads([self::FILE_TAG]);
        $this->clearBuckets([
            self::OUTPUT_BUCKET,
        ]);
    }

    public function testWriteBasicFiles(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');
        file_put_contents($root . '/upload/file2', 'test');
        file_put_contents(
            $root . '/upload/file2.manifest',
            '{"tags": ["' . self::FILE_TAG . '", "xxx"],"is_public": false}'
        );
        file_put_contents($root . '/upload/file3', 'test');
        file_put_contents(
            $root . '/upload/file3.manifest',
            '{"tags": ["' . self::FILE_TAG . '"],"is_public": true}'
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
                'tags' => [self::FILE_TAG],
            ],
            [
                'source' => 'file2',
                'tags' => [self::FILE_TAG, 'another-tag'],
                'is_public' => true,
            ],
        ];

        $writer = new FileWriter($this->getStagingFactory());

        $writer->uploadFiles(
            '/upload',
            ['mapping' => $configs],
            $systemMetadata,
            AbstractStrategyFactory::LOCAL
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([self::FILE_TAG]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
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
            self::FILE_TAG,
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

    public function testWriteFilesOutputMapping(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');

        $configs = [
            [
                'source' => 'file1',
                'tags' => [self::FILE_TAG],
            ],
        ];

        $writer = new FileWriter($this->getStagingFactory());

        $writer->uploadFiles('/upload', ['mapping' => $configs], [], AbstractStrategyFactory::LOCAL);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([self::FILE_TAG]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        self::assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
        }

        self::assertNotNull($file1);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals([self::FILE_TAG], $file1['tags']);
    }

    public function testWriteFilesOutputMappingDevMode(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                null
            )
        );
        $branchName = self::class;
        $branchId = $this->createBranch($clientWrapper, $branchName);
        $this->clearFileUploads([$branchName . '-' . self::FILE_TAG]);
        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
                $branchId
            )
        );

        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');

        $configs = [
            [
                'source' => 'file1',
                'tags' => [self::FILE_TAG],
            ],
        ];

        $writer = new FileWriter($this->getStagingFactory());

        $systemMetadata = [
            'componentId' => 'testComponent',
            'configurationId' => 'metadata-write-test',
            'configurationRowId' => '12345',
            'branchId' => $branchId,
            'runId' => '999',
        ];

        $writer->uploadFiles('/upload', ['mapping' => $configs], $systemMetadata, AbstractStrategyFactory::LOCAL);
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([sprintf('%s-' . self::FILE_TAG, $branchId)]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        self::assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
        }

        $expectedTags = [
            sprintf('%s-' . self::FILE_TAG, $branchId),
            sprintf('%s-componentId: testComponent', $branchId),
            sprintf('%s-configurationId: metadata-write-test', $branchId),
            sprintf('%s-configurationRowId: 12345', $branchId),
            sprintf('%s-branchId: %s', $branchId, $branchId),
            sprintf('%s-runId: 999', $branchId),
        ];

        self::assertNotNull($file1);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals($expectedTags, $file1['tags']);
    }

    public function testWriteFilesOutputMappingAndManifest(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');
        file_put_contents(
            $root . '/upload/file1.manifest',
            '{"tags": ["' . self::FILE_TAG . '", "xxx"],"is_public": true}'
        );

        $configs = [
            [
                'source' => 'file1',
                'tags' => [self::FILE_TAG, 'yyy'],
                'is_public' => false,
            ],
        ];

        $writer = new FileWriter($this->getStagingFactory());
        $writer->uploadFiles(
            'upload',
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            AbstractStrategyFactory::LOCAL
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([self::FILE_TAG]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        self::assertCount(1, $files);

        $file1 = null;
        foreach ($files as $file) {
            if ($file['name'] === 'file1') {
                $file1 = $file;
            }
        }

        self::assertNotNull($file1);
        self::assertEquals(4, $file1['sizeBytes']);
        self::assertEquals([self::FILE_TAG, 'yyy', 'componentId: foo'], $file1['tags']);
        self::assertFalse($file1['isPublic']);
    }

    public function testWriteFilesInvalidJson(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');
        file_put_contents($root . '/upload/file1.manifest', 'this is not at all a {valid} json');

        $configs = [
            [
                'source' => 'file1',
                'tags' => [self::FILE_TAG, 'yyy'],
                'is_public' => false,
            ],
        ];

        $writer = new FileWriter($this->getStagingFactory());
        $writer->setFormat('json');
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('json');
        $writer->uploadFiles('/upload', ['mapping' => $configs], [], AbstractStrategyFactory::LOCAL);
    }

    public function testWriteFilesInvalidYaml(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');
        file_put_contents($root . '/upload/file1.manifest', "\tthis is not \n\t \tat all a {valid} json");

        $configs = [
            [
                'source' => 'file1',
                'tags' => [self::FILE_TAG, 'yyy'],
                'is_public' => false,
            ],
        ];

        $writer = new FileWriter($this->getStagingFactory());
        $writer->setFormat('json');
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('json');
        $writer->uploadFiles('upload', ['mapping' => $configs], [], AbstractStrategyFactory::LOCAL);
    }

    public function testWriteFilesOutputMappingMissing(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/file1', 'test');
        file_put_contents(
            $root . '/upload/file1.manifest',
            '{"tags": ["' . self::FILE_TAG . '-xxx"],"is_public": true}'
        );

        $configs = [
            [
                'source' => 'file2',
                'tags' => [self::FILE_TAG],
                'is_public' => false,
            ],
        ];
        $writer = new FileWriter($this->getStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage("File 'file2' not found");
        $this->expectExceptionCode(404);
        $writer->uploadFiles('upload', ['mapping' => $configs], [], AbstractStrategyFactory::LOCAL);
    }

    public function testWriteFilesOrphanedManifest(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents(
            $root . '/upload/file1.manifest',
            '{"tags": ["' . self::FILE_TAG . '-xxx"],"is_public": true}'
        );
        $writer = new FileWriter($this->getStagingFactory());
        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage("Found orphaned file manifest: 'file1.manifest'");
        $writer->uploadFiles('/upload', [], [], AbstractStrategyFactory::LOCAL);
    }

    public function testWriteFilesNoComponentId(): void
    {
        $writer = new FileWriter($this->getStagingFactory());
        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Component Id must be set');
        $writer->uploadFiles('/upload', [], ['configurationId' => '123'], AbstractStrategyFactory::LOCAL);
    }

    public function testTagProcessedFiles(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/test', 'test');

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([self::FILE_TAG])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([self::FILE_TAG])
        );
        sleep(1);

        $writer = new FileWriter($this->getStagingFactory());
        $configuration = [['tags' => [self::FILE_TAG], 'processed_tags' => ['downloaded']]];
        $writer->tagFiles($configuration);

        $file = $this->clientWrapper->getBasicClient()->getFile($id1);
        self::assertTrue(in_array('downloaded', $file['tags']));
        $file = $this->clientWrapper->getBasicClient()->getFile($id2);
        self::assertTrue(in_array('downloaded', $file['tags']));
    }

    public function testTagBranchProcessedFiles(): void
    {
        $root = $this->tmp->getTmpFolder();
        file_put_contents($root . '/upload/test', 'test');

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags([self::FILE_TAG])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload/test',
            (new FileUploadOptions())->setTags(['12345-' . self::FILE_TAG])
        );
        sleep(1);
        // set it to use a branch
        $this->initClient('12345');

        $writer = new FileWriter($this->getStagingFactory());
        $configuration = [['tags' => [self::FILE_TAG], 'processed_tags' => ['downloaded']]];
        $writer->tagFiles($configuration);

        // first file shouldn't be marked as processed because a branch file exists
        $file1 = $this->clientWrapper->getBasicClient()->getFile($id1);
        self::assertTrue(!in_array('12345-downloaded', $file1['tags']));
        $file2 = $this->clientWrapper->getBasicClient()->getFile($id2);
        self::assertTrue(in_array('12345-downloaded', $file2['tags']));
        self::assertTrue(in_array('12345-' . self::FILE_TAG, $file2['tags']));
    }

    public function testTableFiles(): void
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
            'tags' => [self::FILE_TAG, 'another-tag'],
            'is_permanent' => true,
        ];

        $writer = new FileWriter($this->getStagingFactory());

        $writer->uploadFiles(
            '/upload',
            [],
            $systemMetadata,
            AbstractStrategyFactory::LOCAL,
            $tableFiles
        );
        sleep(1);

        $options = new ListFilesOptions();
        $options->setTags([self::FILE_TAG]);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        self::assertCount(1, $files);

        $expectedTags = [
            self::FILE_TAG,
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
}
