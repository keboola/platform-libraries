<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\Options\FileUploadOptions;
use Psr\Log\Test\TestLogger;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class DownloadFilesTest extends DownloadFilesTestAbstract
{
    public function testReadFiles(): void
    {
        $testLogger = new TestLogger();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        file_put_contents($root . '/upload_second', 'test');

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG]),
        );
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload_second',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG]),
        );
        sleep(5);

        $reader = new Reader($this->getLocalStagingFactory(logger: $testLogger));
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload_second'));

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $id1 . '_upload.manifest');
        $manifest2 = $adapter->readFromFile($root . '/download/' . $id2 . '_upload_second.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('name', $manifest1);
        self::assertArrayHasKey('created', $manifest1);
        self::assertArrayHasKey('is_public', $manifest1);
        self::assertArrayHasKey('is_encrypted', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertArrayHasKey('max_age_days', $manifest1);
        self::assertArrayHasKey('size_bytes', $manifest1);
        self::assertArrayHasKey('is_sliced', $manifest1);
        self::assertFalse($manifest1['is_sliced']);
        self::assertEquals($id1, $manifest1['id']);
        self::assertEquals($id2, $manifest2['id']);
        self::assertTrue($testLogger->hasInfoThatContains(sprintf('Fetched file "%s_upload".', $id1)));
        self::assertTrue($testLogger->hasInfoThatContains(sprintf('Fetched file "%s_upload_second".', $id2)));
    }

    public function testReadFilesOverwrite(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG]),
        );
        sleep(3);

        $reader = new Reader($this->getLocalStagingFactory());
        // download files for the first time
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        file_put_contents((string) file_get_contents($root . '/download/' . $id1 . '_upload'), 'new data');

        // download files for the second time
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));

        // download files without overwrite
        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('Overwrite cannot be turned off for local mapping.');
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'overwrite' => false]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
    }

    public function testReadFilesTagsFilterRunId(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $reader = new Reader($this->getLocalStagingFactory());
        $fo = new FileUploadOptions();
        $fo->setTags([self::DEFAULT_TEST_FILE_TAG]);

        $this->clientWrapper->getTableAndFileStorageClient()->setRunId('xyz');
        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId('1234567');
        $id3 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $id4 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId('1234567.8901234');
        $id5 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $id6 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        sleep(5);

        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG],
                'filter_by_run_id' => true,
                'overwrite' => true,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );

        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertFalse(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id3 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id4 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id5 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id6 . '_upload'));
    }

    public function testReadFilesIncludeAllTags(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $reader = new Reader($this->getLocalStagingFactory());

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1']);

        $file2 = new FileUploadOptions();
        $file2->setTags(['tag-1', 'tag-2']);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', 'tag-2', 'tag-3']);

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file1);
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file2);
        $id3 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file3);

        sleep(5);

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag-1',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-2',
                            'match' => 'include',
                        ],
                    ],
                ],
                'overwrite' => true,
            ],
        ];

        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id3 . '_upload'));
    }

    public function testReadFilesIncludeExcludeTags(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $reader = new Reader($this->getLocalStagingFactory());

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1', 'tag-3']);

        $file2 = new FileUploadOptions();
        $file2->setTags(['tag-1', 'tag-3']);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', 'tag-2', 'tag-3']);

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file1);
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file2);
        $id3 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file3);

        sleep(5);

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag-1',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-3',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-2',
                            'match' => 'exclude',
                        ],
                    ],
                ],
                'overwrite' => true,
            ],
        ];

        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertTrue(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertFalse(file_exists($root . '/download/' . $id3 . '_upload'));
    }

    public function testReadFilesIncludeAllTagsWithLimit(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $reader = new Reader($this->getLocalStagingFactory());

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1', 'tag-2']);

        $file2 = new FileUploadOptions();
        $file2->setTags(['tag-1', 'tag-2']);

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file1);
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file2);

        sleep(5);

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag-1',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-2',
                            'match' => 'include',
                        ],
                    ],
                ],
                'limit' => 1,
                'overwrite' => true,
            ],
        ];

        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
    }
    public function testReadFilesEsQueryFilterRunId(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $reader = new Reader($this->getLocalStagingFactory());
        $fo = new FileUploadOptions();
        $fo->setTags([self::DEFAULT_TEST_FILE_TAG]);

        $this->clientWrapper->getTableAndFileStorageClient()->setRunId('xyz');
        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId('1234567');
        $id3 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $id4 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $this->clientWrapper->getTableAndFileStorageClient()->setRunId('1234567.8901234');
        $id5 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $id6 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        sleep(5);

        $configuration = [
            [
                'query' => 'tags: ' . self::DEFAULT_TEST_FILE_TAG,
                'filter_by_run_id' => true,
                'overwrite' => true,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertFalse(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id3 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id4 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id5 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id6 . '_upload'));
    }

    public function testReadFilesLimit(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/download');

        // make at least 100 files in the project
        for ($i = 0; $i < 102; $i++) {
            $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
                $root . '/upload',
                (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG]),
            );
        }
        sleep(5);

        // valid configuration, but does nothing
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = [];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        // invalid configuration
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = [[]];
        try {
            $reader->downloadFiles(
                $configuration,
                'download',
                AbstractStrategyFactory::LOCAL,
                new InputFileStateList([]),
            );
            self::fail('Invalid configuration should fail.');
        } catch (InvalidInputException) {
        }

        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = [['query' => 'id:>0 AND (NOT tags:table-export)', 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        $finder = new Finder();
        $finder->files()->in($this->temp->getTmpFolder() . '/download')->notName('*.manifest');
        self::assertEquals(100, $finder->count());

        $fs = new Filesystem();
        $fs->remove($this->temp->getTmpFolder());
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'limit' => 102, 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        $finder = new Finder();
        $finder->files()->in($this->temp->getTmpFolder() . '/download')->notName('*.manifest');
        self::assertEquals(102, $finder->count());
    }

    #[NeedsTestTables]
    public function testReadSlicedFileSnowflake(): void
    {
        // Create table and export it to produce a sliced file
        $table = $this->clientWrapper->getTableAndFileStorageClient()->exportTableAsync($this->firstTableId);
        sleep(2);
        $fileId = $table['file']['id'];

        $reader = new Reader($this->getLocalStagingFactory($this->clientWrapper));
        $configuration = [['query' => 'id: ' . $fileId, 'overwrite' => true]];

        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );

        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/download');
        $downloadDir = $this->temp->getTmpFolder() . '/download';
        $fileName = sprintf('%s_%s.csv', $fileId, $this->firstTableId);
        $resultFileContent = '';
        $finder = new Finder();

        /** @var SplFileInfo $file */
        foreach ($finder->files()->in($downloadDir . '/' . $fileName) as $file) {
            $resultFileContent .= file_get_contents($file->getPathname());
        }

        self::assertEquals(
            // phpcs:ignore Generic.Files.LineLength
            "\"id1\",\"name1\",\"foo1\",\"bar1\"\n\"id2\",\"name2\",\"foo2\",\"bar2\"\n\"id3\",\"name3\",\"foo3\",\"bar3\"\n",
            $resultFileContent,
        );

        $manifestFile = $downloadDir . '/' . $fileName . '.manifest';
        self::assertFileExists($manifestFile);
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestFile);
        self::assertArrayHasKey('is_sliced', $manifest);
        self::assertTrue($manifest['is_sliced']);
    }

    public function testReadFilesEmptySlices(): void
    {
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('empty_file');
        $uploadFileId = $this->clientWrapper->getTableAndFileStorageClient()->uploadSlicedFile([], $fileUploadOptions);
        sleep(5);

        $reader = new Reader($this->getLocalStagingFactory($this->clientWrapper));
        $configuration = [
            [
                'query' => 'id:' . $uploadFileId,
                'overwrite' => true,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile(
            $this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file.manifest',
        );
        self::assertEquals($uploadFileId, $manifest['id']);
        self::assertEquals('empty_file', $manifest['name']);
        self::assertDirectoryExists($this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file');
    }

    public function testReadFilesYamlFormat(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG]),
        );
        sleep(5);

        $reader = new Reader($this->getLocalStagingFactory($this->clientWrapper, 'yaml'));
        $configuration = [[
            'tags' => [self::DEFAULT_TEST_FILE_TAG],
            'overwrite' => true,
        ]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $id . '_upload'));

        $adapter = new Adapter('yaml');
        $manifest = $adapter->readFromFile($root . '/download/' . $id . '_upload.manifest');
        self::assertArrayHasKey('id', $manifest);
        self::assertArrayHasKey('name', $manifest);
        self::assertArrayHasKey('created', $manifest);
        self::assertArrayHasKey('is_public', $manifest);
        self::assertArrayHasKey('is_encrypted', $manifest);
    }

    public function testReadFilesWithFileIdsFilter(): void
    {
        $testLogger = new TestLogger();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        file_put_contents($root . '/upload_second', 'test');

        $id1 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG]),
        );
        $id2 = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload_second',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG]),
        );
        sleep(5);

        $reader = new Reader($this->getLocalStagingFactory(logger: $testLogger));
        $configuration = [['file_ids' => [$id1, $id2], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload_second'));

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $id1 . '_upload.manifest');
        $manifest2 = $adapter->readFromFile($root . '/download/' . $id2 . '_upload_second.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('name', $manifest1);
        self::assertArrayHasKey('created', $manifest1);
        self::assertArrayHasKey('is_public', $manifest1);
        self::assertArrayHasKey('is_encrypted', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertArrayHasKey('max_age_days', $manifest1);
        self::assertArrayHasKey('size_bytes', $manifest1);
        self::assertArrayHasKey('is_sliced', $manifest1);
        self::assertFalse($manifest1['is_sliced']);
        self::assertEquals($id1, $manifest1['id']);
        self::assertEquals($id2, $manifest2['id']);
        self::assertTrue($testLogger->hasInfoThatContains(sprintf('Fetched file "%s_upload".', $id1)));
        self::assertTrue($testLogger->hasInfoThatContains(sprintf('Fetched file "%s_upload_second".', $id2)));
    }
}
