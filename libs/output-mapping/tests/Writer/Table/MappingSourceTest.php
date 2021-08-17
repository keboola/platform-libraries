<?php

namespace Keboola\OutputMapping\Tests\Writer\Table;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Finder\SplFileInfo;

class MappingSourceTest extends TestCase
{
    public function testSourceNameMustBeString()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $sourceName must be a string, boolean given');

        new MappingSource(false, 'sourceId');
    }

    public function testSourceIdMustBeString()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $sourceId must be a string, NULL given');

        new MappingSource('sourceName', null);
    }

    public function testGetters()
    {
        $manifestFile = new SplFileInfo('', '', '');
        $mapping = ['a' => 'b'];

        $source = new MappingSource(
            'sourceName',
            'sourceId',
            $manifestFile,
            $mapping
        );

        self::assertSame('sourceName', $source->getName());
        self::assertSame('sourceId', $source->getId());
        self::assertSame($manifestFile, $source->getManifestFile());
        self::assertSame($mapping, $source->getMapping());
    }
}
