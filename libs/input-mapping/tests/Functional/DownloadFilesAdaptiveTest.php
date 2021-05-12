<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\Test\TestLogger;

class DownloadFilesAdaptiveTest extends DownloadFilesTestAbstract
{
    public function testReadFilesAdaptiveWithTags()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive'])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 2'])
        );
        sleep(2);

        $reader = new Reader($this->getStagingFactory());
        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG, 'adaptive'],
                'changed_since' => 'adaptive',
            ]
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $convertedTags = [
            [
                'name' => self::DEFAULT_TEST_FILE_TAG
            ], [
                'name' => 'adaptive'
            ],
        ];
        $fileState = $outputStateList->getFile($convertedTags);
        self::assertEquals($id2, $fileState->getLastImportId());

        self::assertEquals("test", file_get_contents($root . "/download/" . $id1 . '_upload'));
        self::assertFileExists($root . "/download/" . $id1 . '_upload.manifest');
        self::assertEquals("test", file_get_contents($root . "/download/" . $id2 . '_upload'));
        self::assertFileExists($root . "/download/" . $id2 . '_upload.manifest');

        // now load some new files and make sure we just grab the latest since the last run
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 3'])
        );
        $id4 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 4'])
        );
        sleep(2);

        // on the second run we use the state list returned by the first run
        $newOutputStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            StrategyFactory::LOCAL,
            $outputStateList
        );
        $lastFileState = $newOutputStateList->getFile($convertedTags);
        self::assertEquals($id4, $lastFileState->getLastImportId());

        self::assertEquals("test", file_get_contents($root . "/download-adaptive/" . $id3 . '_upload'));
        self::assertFileExists($root . "/download-adaptive/" . $id3 . '_upload.manifest');
        self::assertEquals("test", file_get_contents($root . "/download-adaptive/" . $id4 . '_upload'));
        self::assertFileExists($root . "/download-adaptive/" . $id4 . '_upload.manifest');

        self::assertFileNotExists($root . "/download-adaptive/" . $id1 . '_upload');
        self::assertFileNotExists($root . "/download-adaptive/" . $id1 . '_upload.manifest');
        self::assertFileNotExists($root . "/download-adaptive/" . $id2 . '_upload');
        self::assertFileNotExists($root . "/download-adaptive/" . $id2 . '_upload.manifest');
    }

    public function testReadFilesAdaptiveWithSourceTags()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive'])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 2'])
        );
        $idExclude = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'exclude'])
        );
        sleep(2);

        $reader = new Reader($this->getStagingFactory());
        $sourceConfigTags = [
            [
                'name' => self::DEFAULT_TEST_FILE_TAG,
                'match' => 'include',
            ], [
                'name' => 'adaptive',
                'match' => 'include',
            ], [
                'name' => 'exclude',
                'match' => 'exclude',
            ]
        ];
        $configuration = [
            [
                'source' => [
                    'tags' => $sourceConfigTags,
                ],
                'changed_since' => 'adaptive',
            ]
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $fileState = $outputStateList->getFile($sourceConfigTags);
        self::assertEquals($id2, $fileState->getLastImportId());

        self::assertEquals("test", file_get_contents($root . "/download/" . $id1 . '_upload'));
        self::assertFileExists($root . "/download/" . $id1 . '_upload.manifest');
        self::assertEquals("test", file_get_contents($root . "/download/" . $id2 . '_upload'));
        self::assertFileExists($root . "/download/" . $id2 . '_upload.manifest');
        self::assertFileNotExists($root . "/download/" . $idExclude . '_upload');

        // now load some new files and make sure we just grab the latest since the last run
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 3'])
        );
        $id4 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 4'])
        );
        sleep(2);

        // on the second run we use the state list returned by the first run
        $newOutputStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            StrategyFactory::LOCAL,
            $outputStateList
        );
        $lastFileState = $newOutputStateList->getFile($sourceConfigTags);
        self::assertEquals($id4, $lastFileState->getLastImportId());

        self::assertEquals("test", file_get_contents($root . "/download-adaptive/" . $id3 . '_upload'));
        self::assertFileExists($root . "/download-adaptive/" . $id3 . '_upload.manifest');
        self::assertEquals("test", file_get_contents($root . "/download-adaptive/" . $id4 . '_upload'));
        self::assertFileExists($root . "/download-adaptive/" . $id4 . '_upload.manifest');

        self::assertFileNotExists($root . "/download-adaptive/" . $id1 . '_upload');
        self::assertFileNotExists($root . "/download-adaptive/" . $id1 . '_upload.manifest');
        self::assertFileNotExists($root . "/download-adaptive/" . $id2 . '_upload');
        self::assertFileNotExists($root . "/download-adaptive/" . $id2 . '_upload.manifest');
        self::assertFileNotExists($root . "/download-adaptive/" . $idExclude . '_upload');
    }

    public function testReadFilesAdaptiveWithBranch()
    {
        $clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = $branches->createBranch('my-branch')['id'];
        $clientWrapper->setBranchId($branchId);

        $root = $this->tmpDir;
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
        sleep(2);

        $convertedTags = [
            [
                'name' => self::TEST_FILE_TAG_FOR_BRANCH
            ], [
                'name' => 'adaptive'
            ],
        ];

        $testLogger = new TestLogger();
        $reader = new Reader($this->getStagingFactory($clientWrapper, 'json', $testLogger));

        $configuration = [
            [
                'tags' => [self::TEST_FILE_TAG_FOR_BRANCH, 'adaptive'],
                'changed_since' => 'adaptive',
            ]
        ];
        $outputStateFileList = $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $lastFileState = $outputStateFileList->getFile($convertedTags);
        self::assertEquals($file1Id, $lastFileState->getLastImportId());
        self::assertEquals("test", file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertFileNotExists($root . '/download/' . $file2Id . '_upload');

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $file1Id . '_upload.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertEquals($file1Id, $manifest1['id']);
        self::assertEquals([$branchTag], $manifest1['tags']);

        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf(
                'Using dev tags "%s-%s, %s-adaptive" instead of "%s, adaptive".',
                $branchId,
                self::TEST_FILE_TAG_FOR_BRANCH,
                $branchId,
                self::TEST_FILE_TAG_FOR_BRANCH
            )
        ));
        // add another valid file and assert that it gets downloaded and the previous doesn't
        $file3Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, sprintf('%s-adaptive', $branchId)])
        );
        sleep(2);
        $newOutputStateFileList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            StrategyFactory::LOCAL,
            $outputStateFileList
        );
        $lastFileState = $newOutputStateFileList->getFile($convertedTags);
        self::assertEquals($file3Id, $lastFileState->getLastImportId());
        self::assertEquals("test", file_get_contents($root . '/download-adaptive/' . $file3Id . '_upload'));
        self::assertFileNotExists($root . '/download-adaptive/' . $file1Id . '_upload');
    }

    public function testAdaptiveNoMatchingFiles()
    {
        $this->clientWrapper->setBranchId('');

        $reader = new Reader($this->getStagingFactory());
        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG, 'adaptive'],
                'changed_since' => 'adaptive',
            ]
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $this->assertEmpty($outputStateList->jsonSerialize());
    }

    public function testAdaptiveNoMatchingNewFiles()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive'])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 2'])
        );
        sleep(2);

        $reader = new Reader($this->getStagingFactory());
        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG, 'adaptive'],
                'changed_since' => 'adaptive',
            ]
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $convertedTags = [
            [
                'name' => self::DEFAULT_TEST_FILE_TAG
            ], [
                'name' => 'adaptive'
            ],
        ];
        $fileState = $outputStateList->getFile($convertedTags);
        self::assertEquals($id2, $fileState->getLastImportId());

        self::assertEquals("test", file_get_contents($root . "/download/" . $id1 . '_upload'));
        self::assertFileExists($root . "/download/" . $id1 . '_upload.manifest');
        self::assertEquals("test", file_get_contents($root . "/download/" . $id2 . '_upload'));
        self::assertFileExists($root . "/download/" . $id2 . '_upload.manifest');

        // now run again with no new files to fetch
        $newOutputStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            StrategyFactory::LOCAL,
            $outputStateList
        );
        $lastFileState = $newOutputStateList->getFile($convertedTags);
        self::assertEquals($id2, $lastFileState->getLastImportId());
    }

    public function testChangedSinceWithDeprecatedTags()
    {

    }
}
