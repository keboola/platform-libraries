<?php

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

class ManifestHelperTest extends TestCase
{
    public function testListManifests()
    {
        $temp = new Temp();
        $temp->initRunFolder();
        mkdir($temp->getTmpFolder() . '/sub-dir');
        touch($temp->getTmpFolder() . '/my.manifest');
        touch($temp->getTmpFolder() . '/my.other.file.manifest');
        touch($temp->getTmpFolder() . '/sub-dir/my.other.file.manifest');
        self::assertEquals(
            [
                $temp->getTmpFolder()  . DIRECTORY_SEPARATOR . 'my.manifest',
                $temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'my.other.file.manifest',
            ],
            ManifestHelper::getManifestFiles($temp->getTmpFolder())
        );
    }
}