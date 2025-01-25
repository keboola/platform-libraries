<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\File;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\SourceType;
use PHPUnit\Framework\TestCase;

class FileItemTest extends TestCase
{
    public function testAccessors(): void
    {
        $item = new FileItem('foo', 'bar', 'kochba', false);
        self::assertEquals('foo', $item->getPathName());
        self::assertEquals('bar', $item->getPath());
        self::assertEquals('kochba', $item->getName());
        self::assertFalse($item->isSliced());
        self::assertSame(SourceType::FILE, $item->getSourceType());

        try {
            $item->getWorkspaceId();
            self::fail('Exception should be thrown');
        } catch (InvalidOutputException $e) {
            self::assertEquals('Not implemented', $e->getMessage());
        }

        try {
            $item->getDataObject();
            self::fail('Exception should be thrown');
        } catch (InvalidOutputException $e) {
            self::assertEquals('Not implemented', $e->getMessage());
        }
    }
}
