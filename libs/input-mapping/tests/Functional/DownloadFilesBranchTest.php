<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class DownloadFilesBranchTest extends DownloadFilesTestAbstract
{
    protected function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                $branchId
            ),
        );
    }

    public function testReadFilesIncludeAllTagsWithBranchOverwrite(): void
    {
        $branches = new DevBranches($this->getClientWrapper(null)->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = (string) $branches->createBranch('my-branch')['id'];
        $clientWrapper = $this->getClientWrapper($branchId);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1']);

        $file2 = new FileUploadOptions();
        $file2->setTags([sprintf('%s-tag-1', $branchId), sprintf('%s-tag-2', $branchId)]);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', sprintf('%s-tag-2', $branchId)]);

        $id1 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file1);
        $id2 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file2);
        $id3 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file3);

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

        $testLogger = new TestLogger();
        $reader = new Reader($this->getLocalStagingFactory($clientWrapper, 'json', $testLogger));
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertFalse(file_exists($root . '/download/' . $id3 . '_upload'));

        self::assertTrue(
            $testLogger->hasInfoThatContains(
                sprintf(
                    'Using dev source tags "%s" instead of "tag-1, tag-2".',
                    implode(', ', [sprintf('%s-tag-1', $branchId), sprintf('%s-tag-2', $branchId)])
                )
            )
        );
    }

    public function testReadAndDownloadFilesWithEsQueryIsRestrictedForBranch(): void
    {
        $branches = new DevBranches($this->getClientWrapper(null)->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }
        $branchId = (string) $branches->createBranch('my-branch')['id'];
        $clientWrapper = $this->getClientWrapper($branchId);

        $reader = new Reader($this->getLocalStagingFactory($clientWrapper));

        $fileConfiguration = ['query' => 'tags: ' . self::DEFAULT_TEST_FILE_TAG];

        try {
            $reader->downloadFiles(
                [$fileConfiguration],
                'dummy',
                AbstractStrategyFactory::LOCAL,
                new InputFileStateList([])
            );
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame(
                "Invalid file mapping, the 'query' attribute is unsupported in the dev/branch context.",
                $e->getMessage()
            );
        }

        try {
            Reader::getFiles($fileConfiguration, $clientWrapper, new NullLogger(), new InputFileStateList([]));
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame(
                "Invalid file mapping, the 'query' attribute is unsupported in the dev/branch context.",
                $e->getMessage()
            );
        }
    }

    public function testReadFilesForBranch(): void
    {
        $branches = new DevBranches($this->getClientWrapper(null)->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = (string) $branches->createBranch('my-branch')['id'];
        $clientWrapper = $this->getClientWrapper($branchId);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-%s', $branchId, self::TEST_FILE_TAG_FOR_BRANCH);

        $file1Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag])
        );
        $file2Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::TEST_FILE_TAG_FOR_BRANCH])
        );
        sleep(5);

        $testLogger = new TestLogger();
        $reader = new Reader($this->getLocalStagingFactory($clientWrapper, 'json', $testLogger));

        $configuration = [[
            'tags' => [self::TEST_FILE_TAG_FOR_BRANCH],
            'overwrite' => true,
        ]];
        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertFileDoesNotExist($root . '/download/' . $file2Id . '_upload');

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $file1Id . '_upload.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertEquals($file1Id, $manifest1['id']);
        self::assertEquals([$branchTag], $manifest1['tags']);

        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "%s".', $branchTag, self::TEST_FILE_TAG_FOR_BRANCH)
        ));
    }

    public function testReadFilesForBranchWithProcessedTags(): void
    {
        $branches = new DevBranches($this->getClientWrapper(null)->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = (string) $branches->createBranch('my-branch')['id'];
        $clientWrapper = $this->getClientWrapper($branchId);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-%s', $branchId, self::TEST_FILE_TAG_FOR_BRANCH);

        $processedTag = sprintf('processed-%s', self::TEST_FILE_TAG_FOR_BRANCH);
        $branchProcessedTag = sprintf('%s-processed-%s', $branchId, self::TEST_FILE_TAG_FOR_BRANCH);
        $excludeTag = sprintf('exclude-%s', self::TEST_FILE_TAG_FOR_BRANCH);

        $file1Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag])
        );
        $file2Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::TEST_FILE_TAG_FOR_BRANCH])
        );
        $processedFileId = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $processedTag])
        );
        $branchProcessedFileId = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $branchProcessedTag])
        );
        $excludeFileId = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $excludeTag])
        );
        sleep(5);

        $testLogger = new TestLogger();
        $reader = new Reader($this->getLocalStagingFactory($clientWrapper, 'json', $testLogger));

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => self::TEST_FILE_TAG_FOR_BRANCH,
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
            new InputFileStateList([])
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertEquals('test', file_get_contents($root . '/download/' . $processedFileId . '_upload'));
        self::assertFileDoesNotExist($root . '/download/' . $file2Id . '_upload');
        self::assertFileDoesNotExist($root . '/download/' . $excludeFileId . '_upload');
        self::assertFileDoesNotExist($root . '/download/' . $branchProcessedFileId . '_upload');

        $this->assertManifestTags(
            $root . '/download/' . $file1Id . '_upload.manifest',
            [$branchTag]
        );
        $this->assertManifestTags(
            $root . '/download/' . $processedFileId . '_upload.manifest',
            [$branchTag, $processedTag]
        );

        $clientWrapper->getBasicClient()->deleteFile($file1Id);
        $clientWrapper->getBasicClient()->deleteFile($excludeFileId);
        $clientWrapper->getBasicClient()->deleteFile($processedFileId);
        $clientWrapper->getBasicClient()->deleteFile($branchProcessedFileId);
    }

    private function assertManifestTags(string $manifestPath, array $tags): void
    {
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestPath);
        self::assertArrayHasKey('tags', $manifest);
        self::assertEquals($tags, $manifest['tags']);
    }
}
