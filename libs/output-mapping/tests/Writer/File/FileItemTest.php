<?php

namespace Keboola\OutputMapping\Tests\Writer\File;

use Keboola\OutputMapping\Writer\File\FileItem;
use PHPUnit\Framework\TestCase;

class FileItemTest extends TestCase
{
    public function testAccessors()
    {
        $item = new FileItem('foo', 'bar', 'kochba');
        self::assertEquals('foo', $item->getPathName());
        self::assertEquals('bar', $item->getPath());
        self::assertEquals('kochba', $item->getName());
    }
}
