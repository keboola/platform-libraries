<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Options\FileUploadOptions;

class DownloadFilesAdaptiveTest extends AbstractDownloadFilesTest
{
    public function testReadFilesAdaptiveWithTags(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive']),
        );
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'test 2']),
        );
        sleep(2);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [
            [
                'tags' => [$this->testFileTag, $this->testFileTag . '-adaptive'],
                'changed_since' => 'adaptive',
                'overwrite' => true,
            ],
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );
        $convertedTags = [
            [
                'name' => $this->testFileTag,
            ], [
                'name' => $this->testFileTag . '-adaptive',
            ],
        ];
        $fileState = $outputStateList->getFile($convertedTags);
        self::assertEquals($id2, $fileState->getLastImportId());

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertFileExists($root . '/download/' . $id1 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload'));
        self::assertFileExists($root . '/download/' . $id2 . '_upload.manifest');

        // now load some new files and make sure we just grab the latest since the last run
        $id3 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'test 3']),
        );
        $id4 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'test 4']),
        );
        sleep(2);

        // on the second run we use the state list returned by the first run
        $newOutputStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            $outputStateList,
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
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive']),
        );
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'test 2']),
        );
        $idExclude = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'exclude']),
        );
        sleep(2);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $sourceConfigTags = [
            [
                'name' => $this->testFileTag,
                'match' => 'include',
            ], [
                'name' => $this->testFileTag . '-adaptive',
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
            new InputFileStateList([]),
        );
        $fileState = $outputStateList->getFile($sourceConfigTags);
        self::assertEquals($id2, $fileState->getLastImportId());

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertFileExists($root . '/download/' . $id1 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload'));
        self::assertFileExists($root . '/download/' . $id2 . '_upload.manifest');
        self::assertFileDoesNotExist($root . '/download/' . $idExclude . '_upload');

        // now load some new files and make sure we just grab the latest since the last run
        $id3 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'test 3']),
        );
        $id4 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'test 4']),
        );
        sleep(2);

        // on the second run we use the state list returned by the first run
        $newOutputStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            $outputStateList,
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
        $clientWrapper = $this->initClient();
        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [
            [
                'tags' => [$this->testFileTag, $this->testFileTag . '-adaptive'],
                'changed_since' => 'adaptive',
                'overwrite' => true,
            ],
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );

        self::assertEmpty($outputStateList->jsonSerialize());
    }

    public function testAdaptiveNoMatchingNewFiles(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive']),
        );
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'test 2']),
        );
        sleep(2);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [
            [
                'tags' => [$this->testFileTag, $this->testFileTag . '-adaptive'],
                'changed_since' => 'adaptive',
                'overwrite' => true,
            ],
        ];
        // on the first run the state list will be empty
        $outputStateList = $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );
        $convertedTags = [
            [
                'name' => $this->testFileTag,
            ], [
                'name' => $this->testFileTag . '-adaptive',
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
            $outputStateList,
        );
        $lastFileState = $newOutputStateList->getFile($convertedTags);
        self::assertEquals($id2, $lastFileState->getLastImportId());
    }

    public function testChangedSinceNonAdaptive(): void
    {
        $clientWrapper = $this->initClient();
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive']),
        );
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTag, $this->testFileTag . '-adaptive', 'test 2']),
        );
        sleep(2);

        $reader = new Reader(
            $clientWrapper,
            $this->testLogger,
            $this->getLocalStagingFactory($clientWrapper),
        );
        $configuration = [
            [
                'tags' => [$this->testFileTag, $this->testFileTag . '-adaptive'],
                'changed_since' => '-5 minutes',
                'overwrite' => true,
            ],
        ];

        $reader->downloadFiles(
            $configuration,
            'download',
            new InputFileStateList([]),
        );

        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        self::assertFileExists($root . '/download/' . $id1 . '_upload.manifest');
        self::assertEquals('test', file_get_contents($root . '/download/' . $id2 . '_upload'));
        self::assertFileExists($root . '/download/' . $id2 . '_upload.manifest');
    }
}
