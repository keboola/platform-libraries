<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Strategy\Local;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\Settle\SettleFactory;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\Branch;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Psr\Log\NullLogger;
use RuntimeException;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class DownloadFilesTest extends AbstractDownloadFilesTest
{
    public function testReadFiles(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        file_put_contents($root . '/upload_second', 'test');

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload_second',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        sleep(5);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory(
                clientWrapper: $clientWrapper,
                logger: $this->testLogger,
            ),
        );
        $configuration = [['tags' => [$this->testFileTag], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
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
        // info level: the unchanged, greppable contract messages
        self::assertTrue($this->testHandler->hasInfoThatContains(sprintf('Fetched file "%s_upload".', $id1)));
        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf('Fetched file "%s_upload_second".', $id2),
        ));
        self::assertTrue($this->testHandler->hasInfoThatContains('All files were fetched.'));

        // debug level: the download size and duration detail added by AJDA-3148
        self::assertTrue($this->testHandler->hasDebugThatContains(
            sprintf('Fetched file "%s_upload". Downloaded ', $id1),
        ));
        self::assertTrue($this->testHandler->hasDebugThatContains(
            sprintf('Fetched file "%s_upload_second". Downloaded ', $id2),
        ));
        self::assertTrue($this->testHandler->hasDebugThatContains('All files were fetched. 2 files, '));
    }

    public function testReadFilesOverwrite(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        sleep(3);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        // download files for the first time
        $configuration = [['tags' => [$this->testFileTag], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        file_put_contents((string) file_get_contents($root . '/download/' . $id1 . '_upload'), 'new data');

        // download files for the second time
        $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));

        // download files without overwrite
        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('Overwrite cannot be turned off for local mapping.');
        $configuration = [['tags' => [$this->testFileTag], 'overwrite' => false]];
        $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );
    }

    public function testReadFilesIncludeAllTags(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1']);

        $file2 = new FileUploadOptions();
        $file2->setTags(['tag-1', 'tag-2']);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', 'tag-2', 'tag-3']);

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file1);
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file2);
        $id3 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file3);

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
            new InputFileStateList([]),
        );
        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id3 . '_upload'));
    }

    public function testReadFilesIncludeExcludeTags(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1', 'tag-3']);

        $file2 = new FileUploadOptions();
        $file2->setTags(['tag-1', 'tag-3']);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', 'tag-2', 'tag-3']);

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file1);
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file2);
        $id3 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file3);

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
            new InputFileStateList([]),
        );
        self::assertTrue(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertFalse(file_exists($root . '/download/' . $id3 . '_upload'));
    }

    public function testReadFilesIncludeAllTagsWithLimit(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1', 'tag-2']);

        $file2 = new FileUploadOptions();
        $file2->setTags(['tag-1', 'tag-2']);

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file1);
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $file2);

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
            new InputFileStateList([]),
        );
        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
    }

    public function testReadFilesLimit(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/download');

        // make at least 100 files in the project
        for ($i = 0; $i < 102; $i++) {
            $clientWrapper->getTableAndFileStorageClient()->uploadFile(
                $root . '/upload',
                (new FileUploadOptions())->setTags([$this->testFileTag]),
            );
        }

        $settleFactory = new SettleFactory(new NullLogger());
        $settle = $settleFactory->createSettle(10, 2);

        $settle->settle(
            function (int $expectedFilesCount) use ($clientWrapper): bool {
                $files = $clientWrapper->getTableAndFileStorageClient()->listFiles(
                    (new ListFilesOptions())
                        ->setTags([$this->testFileTag])
                        ->setLimit(102),
                );
                return count($files) === $expectedFilesCount;
            },
            function (): int {
                return 102;
            },
        );

        // valid configuration, but does nothing
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [];
        $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );

        // invalid configuration
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [[]];
        try {
            $reader->downloadFiles(
                $configuration,
                'download',
                new InputFileStateList([]),
            );
            self::fail('Invalid configuration should fail.');
        } catch (InvalidInputException) {
        }

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [['query' => 'id:>0 AND (NOT tags:table-export)', 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );
        $finder = new Finder();
        $finder->files()->in($this->temp->getTmpFolder() . '/download')->notName('*.manifest');
        self::assertEquals(100, $finder->count());

        $fs = new Filesystem();
        $fs->remove($this->temp->getTmpFolder());

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [['tags' => [$this->testFileTag], 'limit' => 102, 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );
        $finder = new Finder();
        $finder->files()->in($this->temp->getTmpFolder() . '/download')->notName('*.manifest');
        self::assertEquals(102, $finder->count());
    }

    #[NeedsTestTables]
    public function testReadSlicedFileSnowflake(): void
    {
        $clientWrapper = $this->initClient();
        // Create table and export it to produce a sliced file
        $table = $clientWrapper->getTableAndFileStorageClient()->exportTableAsync($this->firstTableId);
        sleep(2);
        $fileId = $table['file']['id'];

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [['query' => 'id: ' . $fileId, 'overwrite' => true]];

        $reader->downloadFiles(
            $configuration,
            'download',
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
        $clientWrapper = $this->initClient();
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('empty_file');
        $uploadFileId = $clientWrapper->getTableAndFileStorageClient()->uploadSlicedFile([], $fileUploadOptions);
        sleep(5);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [
            [
                'query' => 'id:' . $uploadFileId,
                'overwrite' => true,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
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
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        sleep(5);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory(
                clientWrapper: $clientWrapper,
                format: FileFormat::Yaml,
            ),
        );
        $configuration = [[
            'tags' => [$this->testFileTag],
            'overwrite' => true,
        ]];
        $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $id . '_upload'));

        $adapter = new Adapter(FileFormat::Yaml);
        $manifest = $adapter->readFromFile($root . '/download/' . $id . '_upload.manifest');
        self::assertArrayHasKey('id', $manifest);
        self::assertArrayHasKey('name', $manifest);
        self::assertArrayHasKey('created', $manifest);
        self::assertArrayHasKey('is_public', $manifest);
        self::assertArrayHasKey('is_encrypted', $manifest);
    }

    public function testReadFilesWithFileIdsFilter(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        file_put_contents($root . '/upload_second', 'test');

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload_second',
            (new FileUploadOptions())->setTags([$this->testFileTag]),
        );
        sleep(5);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory(
                clientWrapper: $clientWrapper,
                logger: $this->testLogger,
            ),
        );
        $configuration = [['file_ids' => [$id1, $id2], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
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
        self::assertTrue($this->testHandler->hasInfoThatContains(sprintf('Fetched file "%s_upload".', $id1)));
        self::assertTrue($this->testHandler->hasInfoThatContains(sprintf('Fetched file "%s_upload_second".', $id2)));
    }

    public function testStorageClientExceptionIsReportedAsUserError(): void
    {
        $storageClient = $this->createDownloadFailingStorageClient(function (): void {
            throw new ClientException(
                'Cannot download file "my-file.csv" (ID 123) from Storage, ' .
                'please verify the contents of the file and that the file has not expired.',
                404,
            );
        });
        $strategy = $this->createLocalStrategyWithStorageClient($storageClient);

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Failed to download file my-file.csv (123): Cannot download file "my-file.csv" (ID 123) from Storage, ' .
            'please verify the contents of the file and that the file has not expired.',
        );

        $strategy->downloadFiles([['tags' => ['my-tag'], 'overwrite' => true]], 'download');
    }

    public function testUnexpectedErrorIsReportedAsApplicationError(): void
    {
        $storageClient = $this->createDownloadFailingStorageClient(function (): void {
            throw new RuntimeException('Something went wrong internally.');
        });
        $strategy = $this->createLocalStrategyWithStorageClient($storageClient);

        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage(
            'Failed to download file my-file.csv (123): Something went wrong internally.',
        );

        $strategy->downloadFiles([['tags' => ['my-tag'], 'overwrite' => true]], 'download');
    }

    private function createDownloadFailingStorageClient(callable $downloadFileCallback): BranchAwareClient
    {
        $storageClient = $this->createMock(BranchAwareClient::class);
        $storageClient->method('listFiles')->willReturn([['id' => 123, 'name' => 'my-file.csv']]);
        $storageClient->method('getFile')->willReturn([
            'id' => 123,
            'name' => 'my-file.csv',
            'isSliced' => false,
        ]);
        $storageClient->method('downloadFile')->willReturnCallback($downloadFileCallback);

        return $storageClient;
    }

    private function createLocalStrategyWithStorageClient(BranchAwareClient $storageClient): Local
    {
        $basicClient = $this->createMock(Client::class);
        $basicClient->method('getRunId')->willReturn('run-123');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->method('isDevelopmentBranch')->willReturn(false);
        $clientWrapper->method('getClientOptionsReadOnly')->willReturn(new ClientOptions());
        $clientWrapper->method('getTableAndFileStorageClient')->willReturn($basicClient);
        $clientWrapper->method('getClientForBranch')->willReturn($storageClient);
        $clientWrapper->method('getDefaultBranch')->willReturn(new Branch('123', 'main', true, null));

        $staging = $this->createMock(FileStagingInterface::class);
        $staging->method('getPath')->willReturn($this->temp->getTmpFolder());

        return new Local(
            $clientWrapper,
            new NullLogger(),
            $staging,
            $staging,
            new InputFileStateList([]),
            FileFormat::Json,
        );
    }
}
