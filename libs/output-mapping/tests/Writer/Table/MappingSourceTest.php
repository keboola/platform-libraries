<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Finder\SplFileInfo;

class MappingSourceTest extends TestCase
{
    public function testGetters(): void
    {
        $source = new WorkspaceItemSource('source-name', 'workspace-id', 'data-object', false);
        $manifestFile = new SplFileInfo('', '', '');
        $mapping = ['a' => 'b'];

        $mappingSource = new MappingSource(
            $source,
            $manifestFile,
            $mapping
        );

        self::assertSame($source, $mappingSource->getSource());
        self::assertSame('source-name', $mappingSource->getSourceName());
        self::assertSame($manifestFile, $mappingSource->getManifestFile());
        self::assertSame($mapping, $mappingSource->getMapping());
    }

    public function testSetManifestFile(): void
    {
        $source = new MappingSource(new WorkspaceItemSource('source-name', 'workspace-id', 'data-object', false));
        self::assertNull($source->getManifestFile());

        $manifestFile = new SplFileInfo('', '', '');
        $source->setManifestFile($manifestFile);

        self::assertSame($manifestFile, $source->getManifestFile());
    }

    public function testSetMapping(): void
    {
        $source = new MappingSource(new WorkspaceItemSource('source-name', 'workspace-id', 'data-object', false));
        self::assertNull($source->getMapping());

        $manifestFile = ['a' => 'b'];
        $source->setMapping($manifestFile);

        self::assertSame($manifestFile, $source->getMapping());
    }
}
