<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Tests\Needs\NeedsDevBranch;
use Keboola\StorageApi\Options\FileUploadOptions;

class DownloadFilesAdaptiveBranchTest extends DownloadFilesTestAbstract
{
    #[NeedsDevBranch]
    public function testReadFilesAdaptiveWithBranch(): void
    {
        $this->initClient($this->devBranchId);

        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-%s', $this->devBranchId, $this->testFileTagForBranch);

        $file1Id = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag]),
        );
        $file2Id = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$this->testFileTagForBranch]),
        );
        sleep(2);

        $convertedTags = [
            [
                'name' => $this->testFileTagForBranch,
            ], [
                'name' => 'adaptive',
            ],
        ];

        $reader = new Reader($this->getLocalStagingFactory($this->clientWrapper, 'json', $this->testLogger));

        $configuration = [
            [
                'tags' => [$this->testFileTagForBranch, 'adaptive'],
                'changed_since' => 'adaptive',
                'overwrite' => true,
            ],
        ];
        $outputStateFileList = $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
        $lastFileState = $outputStateFileList->getFile($convertedTags);
        self::assertEquals($file1Id, $lastFileState->getLastImportId());
        self::assertEquals('test', file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertFileDoesNotExist($root . '/download/' . $file2Id . '_upload');

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $file1Id . '_upload.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertEquals($file1Id, $manifest1['id']);
        self::assertEquals([$branchTag], $manifest1['tags']);

        self::assertTrue($this->testHandler->hasInfoThatContains(
            sprintf(
                'Using dev tags "%s-%s, %s-adaptive" instead of "%s, adaptive".',
                $this->devBranchId,
                $this->testFileTagForBranch,
                $this->devBranchId,
                $this->testFileTagForBranch,
            ),
        ));
        // add another valid file and assert that it gets downloaded and the previous doesn't
        $file3Id = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, sprintf('%s-adaptive', $this->devBranchId)]),
        );
        sleep(2);
        $newOutputStateFileList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            AbstractStrategyFactory::LOCAL,
            $outputStateFileList,
        );
        $lastFileState = $newOutputStateFileList->getFile($convertedTags);
        self::assertEquals($file3Id, $lastFileState->getLastImportId());
        self::assertEquals('test', file_get_contents($root . '/download-adaptive/' . $file3Id . '_upload'));
        self::assertFileDoesNotExist($root . '/download-adaptive/' . $file1Id . '_upload');
    }
}
