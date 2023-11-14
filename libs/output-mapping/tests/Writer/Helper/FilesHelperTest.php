<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use SplFileInfo;
use Symfony\Component\Filesystem\Exception\FileNotFoundException;

class FilesHelperTest extends TestCase
{
    public function testGetManifestFiles(): void
    {
        $temp = new Temp();
        mkdir($temp->getTmpFolder() . '/sub-dir');
        touch($temp->getTmpFolder() . '/my.csv');
        touch($temp->getTmpFolder() . '/my.csv.manifest');
        touch($temp->getTmpFolder() . '/my.root.file.manifest');
        touch($temp->getTmpFolder() . '/sub-dir/my.sub-dir.csv');
        touch($temp->getTmpFolder() . '/sub-dir/my.sub-dir.file.manifest');

        $result = FilesHelper::getManifestFiles($temp->getTmpFolder());
        $result = array_map(function (SplFileInfo $file) {
            return $file->getPathname();
        }, $result);
        sort($result);

        self::assertSame(
            [
                $temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'my.csv.manifest',
                $temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'my.root.file.manifest',
            ],
            $result,
        );
    }

    public function testGetNonManifestFiles(): void
    {
        $temp = new Temp();
        mkdir($temp->getTmpFolder() . '/sub-dir');
        touch($temp->getTmpFolder() . '/my.csv');
        touch($temp->getTmpFolder() . '/my.csv.manifest');
        touch($temp->getTmpFolder() . '/my.root.file.manifest');
        touch($temp->getTmpFolder() . '/sub-dir/my.subdir.csv');
        touch($temp->getTmpFolder() . '/sub-dir/my.subdir.file.manifest');

        $result = FilesHelper::getDataFiles($temp->getTmpFolder());
        $result = array_map(function (SplFileInfo $file) {
            return $file->getPathname();
        }, $result);
        sort($result);

        self::assertSame(
            [
                $temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'my.csv',
                $temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'sub-dir',
            ],
            $result,
        );
    }

    public function testGetFile(): void
    {
        $temp = new Temp();

        $filePathName = $temp->getTmpFolder() . '/my.csv';
        touch($filePathName);
        self::assertSame($filePathName, FilesHelper::getFile($filePathName)->getPathname());

        $directoryPathname = $temp->getTmpFolder() . '/sub-dir';
        mkdir($directoryPathname);
        try {
            FilesHelper::getFile($directoryPathname);
            self::fail('getFile for directory path should fail');
        } catch (FileNotFoundException $e) {
            self::assertSame(
                sprintf('File "%s" could not be found.', $directoryPathname),
                $e->getMessage(),
            );
        }

        $filePathName = $temp->getTmpFolder() . '/dummy.csv';
        try {
            FilesHelper::getFile($filePathName);
            self::fail('getFile for non-existing file should fail');
        } catch (FileNotFoundException $e) {
            self::assertSame(
                sprintf('File "%s" could not be found.', $filePathName),
                $e->getMessage(),
            );
        }
    }
}
