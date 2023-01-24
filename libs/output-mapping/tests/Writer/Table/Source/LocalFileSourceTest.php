<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Source;

use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use SplFileInfo;

class LocalFileSourceTest extends TestCase
{
    public function testRegularFile(): void
    {
        $temp = new Temp();
        touch($temp->getTmpFolder() . '/my.csv');

        $file = new SplFileInfo($temp->getTmpFolder() . '/my.csv');
        $source = new LocalFileSource($file);

        self::assertSame($file, $source->getFile());
        self::assertSame('my.csv', $source->getName());
        self::assertFalse($source->isSliced());
    }

    public function testSlicedFile(): void
    {
        $temp = new Temp();
        mkdir($temp->getTmpFolder() . '/my-slices');
        touch($temp->getTmpFolder() . '/my-slices/file1.csv');

        $file = new SplFileInfo($temp->getTmpFolder() . '/my-slices');
        $source = new LocalFileSource($file);

        self::assertSame($file, $source->getFile());
        self::assertSame('my-slices', $source->getName());
        self::assertTrue($source->isSliced());
    }
}
