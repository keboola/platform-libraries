<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;

class DownloadFilesBranchTest extends DownloadFilesTestAbstract
{
    #[NeedsDevBranch]
    public function testReadFilesIncludeAllTagsWithBranchOverwrite(): void
    {
        $clientWrapper = $this->initClient($this->devBranchId);
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1']);

        $file2 = new FileUploadOptions();
        $file2->setTags([sprintf('%s-tag-1', $this->devBranchId), sprintf('%s-tag-2', $this->devBranchId)]);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', sprintf('%s-tag-2', $this->devBranchId)]);

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

        $reader = new Reader($this->getLocalStagingFactory($clientWrapper, 'json', $this->testLogger));
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertFalse(file_exists($root . '/download/' . $id3 . '_upload'));

        self::assertTrue(
            $this->testHandler->hasInfoThatContains(
                sprintf(
                    'Using dev source tags "%s" instead of "tag-1, tag-2".',
                    implode(
                        ', ',
                        [
                            sprintf('%s-tag-1', $this->devBranchId),
                            sprintf('%s-tag-2', $this->devBranchId),
                        ],
                    ),
                ),
            ),
        );
    }

    #[NeedsDevBranch]
    public function testReadAndDownloadFilesWithEsQueryIsRestrictedForBranch(): void
    {
        $clientWrapper = $this->initClient($this->devBranchId);

        $reader = new Reader($this->getLocalStagingFactory($clientWrapper));

        $fileConfiguration = ['query' => 'tags: ' . $this->testFileTag];

        try {
            $reader->downloadFiles(
                [$fileConfiguration],
                'dummy',
                AbstractStrategyFactory::LOCAL,
                new InputFileStateList([]),
            );
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame(
                "Invalid file mapping, the 'query' attribute is unsupported in the dev/branch context.",
                $e->getMessage(),
            );
        }

        try {
            Reader::getFiles($fileConfiguration, $clientWrapper, new NullLogger());
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame(
                "Invalid file mapping, the 'query' attribute is unsupported in the dev/branch context.",
                $e->getMessage(),
            );
        }
    }

    #[NeedsDevBranch]
    public function testReadFilesForBranchFakeDevStorage(): void
    {
        $clientWrapper = $this->initClient($this->devBranchId);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-%s', $this->devBranchId, $this->testFileTagForBranch);

        $file1Id = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag]),
        );
        $file2Id = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTagForBranch]),
        );
        sleep(5);

        $reader = new Reader($this->getLocalStagingFactory($clientWrapper, 'json', $this->testLogger));

        $configuration = [[
            'tags' => [$this->testFileTagForBranch],
            'overwrite' => true,
        ]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertFileDoesNotExist($root . '/download/' . $file2Id . '_upload');

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $file1Id . '_upload.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertEquals($file1Id, $manifest1['id']);
        self::assertEquals([$branchTag], $manifest1['tags']);

        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "%s".', $branchTag, $this->testFileTagForBranch),
        ));
    }

    #[NeedsDevBranch]
    public function testReadFilesForBranchRealDevStorage(): void
    {
        $clientOptions = $this->initClient()->getClientOptionsReadOnly()
            ->setBranchId($this->devBranchId)
            ->setUseBranchStorage(true) // this is the important part
        ;

        $clientWrapper = new ClientWrapper($clientOptions);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $file1Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTagForBranch, 'tag-1']),
        );
        $file2Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTagForBranch, 'tag-2']),
        );
        $file3Id = $clientWrapper->getBranchClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTagForBranch, 'tag-2']),
        );
        sleep(5);

        $reader = new Reader($this->getLocalStagingFactory($clientWrapper, 'json', $this->testLogger));

        $configuration = [
            [
                'tags' => ['tag-1'],
                'overwrite' => true,
            ],
            [
                'tags' => ['tag-2'],
                'overwrite' => true,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertFileDoesNotExist($root . '/download/' . $file2Id . '_upload');
        self::assertEquals('test', file_get_contents($root . '/download/' . $file3Id . '_upload'));

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $file1Id . '_upload.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertEquals($file1Id, $manifest1['id']);
        self::assertEquals([$this->testFileTagForBranch, 'tag-1'], $manifest1['tags']);

        $manifest3 = $adapter->readFromFile($root . '/download/' . $file3Id . '_upload.manifest');

        self::assertArrayHasKey('id', $manifest3);
        self::assertArrayHasKey('tags', $manifest3);
        self::assertEquals($file3Id, $manifest3['id']);
        self::assertEquals([$this->testFileTagForBranch, 'tag-2'], $manifest3['tags']);

        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf(
                'Using files from default branch "%s" for tags "tag-1".',
                $clientWrapper->getDefaultBranch()->id,
            ),
        ));

        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf(
                'Using files from development branch "%s" for tags "tag-2".',
                $clientWrapper->getClientOptionsReadOnly()->getBranchId(),
            ),
        ));
    }

    #[NeedsDevBranch]
    public function testReadFilesForBranchWithProcessedTags(): void
    {
        $clientWrapper = $this->initClient($this->devBranchId);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-%s', $this->devBranchId, $this->testFileTagForBranch);

        $processedTag = sprintf('processed-%s', $this->testFileTagForBranch);
        $branchProcessedTag = sprintf('%s-processed-%s', $this->devBranchId, $this->testFileTagForBranch);
        $excludeTag = sprintf('exclude-%s', $this->testFileTagForBranch);

        $file1Id = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag]),
        );
        $file2Id = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTagForBranch]),
        );
        $processedFileId = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $processedTag]),
        );
        $branchProcessedFileId = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $branchProcessedTag]),
        );
        $excludeFileId = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $excludeTag]),
        );
        sleep(5);

        $reader = new Reader($this->getLocalStagingFactory($clientWrapper, 'json', $this->testLogger));

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => $this->testFileTagForBranch,
                            'match' => 'include',
                        ],
                        [
                            'name' => $excludeTag,
                            'match' => 'exclude',
                        ],
                        [
                            'name' => $processedTag,
                            'match' => 'exclude',
                        ],
                    ],
                ],
                'processed_tags' => [$processedTag],
                'overwrite' => true,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertEquals('test', file_get_contents($root . '/download/' . $processedFileId . '_upload'));
        self::assertFileDoesNotExist($root . '/download/' . $file2Id . '_upload');
        self::assertFileDoesNotExist($root . '/download/' . $excludeFileId . '_upload');
        self::assertFileDoesNotExist($root . '/download/' . $branchProcessedFileId . '_upload');

        $this->assertManifestTags(
            $root . '/download/' . $file1Id . '_upload.manifest',
            [$branchTag],
        );
        $this->assertManifestTags(
            $root . '/download/' . $processedFileId . '_upload.manifest',
            [$branchTag, $processedTag],
        );

        $clientWrapper->getTableAndFileStorageClient()->deleteFile($file1Id);
        $clientWrapper->getTableAndFileStorageClient()->deleteFile($excludeFileId);
        $clientWrapper->getTableAndFileStorageClient()->deleteFile($processedFileId);
        $clientWrapper->getTableAndFileStorageClient()->deleteFile($branchProcessedFileId);
    }

    private function assertManifestTags(string $manifestPath, array $tags): void
    {
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestPath);
        self::assertArrayHasKey('tags', $manifest);
        self::assertEquals($tags, $manifest['tags']);
    }
}
