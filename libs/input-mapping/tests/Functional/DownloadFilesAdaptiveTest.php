<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Options\FileUploadOptions;

class DownloadFilesAdaptiveTest extends DownloadFilesTestAbstract
{
    public function testReadFilesAdaptiveWithTags(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive'])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 2'])
        );
        sleep(2);

        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG, 'adaptive'],
                'changed_since' => 'adaptive',
                'overwrite' => true,
            ],
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $convertedTags = [
            [
                'name' => self::DEFAULT_TEST_FILE_TAG,
            ], [
                'name' => 'adaptive',
            ],
        ];
        $fileState = $outputStateList->getFile($convertedTags);
        self::assertEquals($id2, $fileState->getLastImportId());

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertFileExists($root . '/download/' . $id1 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload'));
        self::assertFileExists($root . '/download/' . $id2 . '_upload.manifest');

        // now load some new files and make sure we just grab the latest since the last run
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 3'])
        );
        $id4 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 4'])
        );
        sleep(2);

        // on the second run we use the state list returned by the first run
        $newOutputStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            AbstractStrategyFactory::LOCAL,
            $outputStateList
        );
        $lastFileState = $newOutputStateList->getFile($convertedTags);
        self::assertEquals($id4, $lastFileState->getLastImportId());

        self::assertEquals('test', file_get_contents($root . '/download-adaptive/' . $id3 . '_upload'));
        self::assertFileExists($root . '/download-adaptive/' . $id3 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download-adaptive/' . $id4 . '_upload'));
        self::assertFileExists($root . '/download-adaptive/' . $id4 . '_upload.manifest');

        self::assertFileDoesNotExist($root . '/download-adaptive/' . $id1 . '_upload');
        self::assertFileDoesNotExist($root . '/download-adaptive/' . $id1 . '_upload.manifest');
        self::assertFileDoesNotExist($root . '/download-adaptive/' . $id2 . '_upload');
        self::assertFileDoesNotExist($root . '/download-adaptive/' . $id2 . '_upload.manifest');
    }

    public function testReadFilesAdaptiveWithSourceTags(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive'])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 2'])
        );
        $idExclude = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'exclude'])
        );
        sleep(2);

        $reader = new Reader($this->getLocalStagingFactory($this->clientWrapper));
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
            ],
        ];
        $configuration = [
            [
                'source' => [
                    'tags' => $sourceConfigTags,
                ],
                'changed_since' => 'adaptive',
                'overwrite' => true,
            ],
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $fileState = $outputStateList->getFile($sourceConfigTags);
        self::assertEquals($id2, $fileState->getLastImportId());

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertFileExists($root . '/download/' . $id1 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload'));
        self::assertFileExists($root . '/download/' . $id2 . '_upload.manifest');
        self::assertFileDoesNotExist($root . '/download/' . $idExclude . '_upload');

        // now load some new files and make sure we just grab the latest since the last run
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 3'])
        );
        $id4 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 4'])
        );
        sleep(2);

        // on the second run we use the state list returned by the first run
        $newOutputStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            AbstractStrategyFactory::LOCAL,
            $outputStateList
        );
        $lastFileState = $newOutputStateList->getFile($sourceConfigTags);
        self::assertEquals($id4, $lastFileState->getLastImportId());

        self::assertEquals('test', file_get_contents($root . '/download-adaptive/' . $id3 . '_upload'));
        self::assertFileExists($root . '/download-adaptive/' . $id3 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download-adaptive/' . $id4 . '_upload'));
        self::assertFileExists($root . '/download-adaptive/' . $id4 . '_upload.manifest');

        self::assertFileDoesNotExist($root . '/download-adaptive/' . $id1 . '_upload');
        self::assertFileDoesNotExist($root . '/download-adaptive/' . $id1 . '_upload.manifest');
        self::assertFileDoesNotExist($root . '/download-adaptive/' . $id2 . '_upload');
        self::assertFileDoesNotExist($root . '/download-adaptive/' . $id2 . '_upload.manifest');
        self::assertFileDoesNotExist($root . '/download-adaptive/' . $idExclude . '_upload');
    }

    public function testAdaptiveNoMatchingFiles(): void
    {
        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG, 'adaptive'],
                'changed_since' => 'adaptive',
                'overwrite' => true,
            ],
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertEmpty($outputStateList->jsonSerialize());
    }

    public function testAdaptiveNoMatchingNewFiles(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive'])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 2'])
        );
        sleep(2);

        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG, 'adaptive'],
                'changed_since' => 'adaptive',
                'overwrite' => true,
            ],
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $convertedTags = [
            [
                'name' => self::DEFAULT_TEST_FILE_TAG,
            ], [
                'name' => 'adaptive',
            ],
        ];
        $fileState = $outputStateList->getFile($convertedTags);
        self::assertEquals($id2, $fileState->getLastImportId());

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertFileExists($root . '/download/' . $id1 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload'));
        self::assertFileExists($root . '/download/' . $id2 . '_upload.manifest');

        // now run again with no new files to fetch
        $newOutputStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            AbstractStrategyFactory::LOCAL,
            $outputStateList
        );
        $lastFileState = $newOutputStateList->getFile($convertedTags);
        self::assertEquals($id2, $lastFileState->getLastImportId());
    }

    public function testChangedSinceNonAdaptive(): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive'])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG, 'adaptive', 'test 2'])
        );
        sleep(2);

        $reader = new Reader($this->getLocalStagingFactory());
        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG, 'adaptive'],
                'changed_since' => '-5 minutes',
                'overwrite' => true,
            ],
        ];

        $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([])
        );

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertFileExists($root . '/download/' . $id1 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload'));
        self::assertFileExists($root . '/download/' . $id2 . '_upload.manifest');
    }
}
