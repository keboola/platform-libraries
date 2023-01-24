<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use SplFileInfo;

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
            $result
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
            $result
        );
    }
}
