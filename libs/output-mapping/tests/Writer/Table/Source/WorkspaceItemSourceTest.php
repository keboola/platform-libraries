<?php

namespace Keboola\OutputMapping\Tests\Writer\Table\Source;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use PHPUnit\Framework\TestCase;

class WorkspaceItemSourceTest extends TestCase
{
    public function testGetters()
    {
        $source = new WorkspaceItemSource('source-name', 'workspace-id', 'data-object', true);

        self::assertSame('source-name', $source->getName());
        self::assertSame('workspace-id', $source->getWorkspaceId());
        self::assertSame('data-object', $source->getDataObject());
        self::assertTrue($source->isSliced());
    }

    public function testSourceNameMustBeString()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $sourceName must be a string, boolean given');

        new WorkspaceItemSource(false, 'workspace-id', 'data-object', false);
    }

    public function testWorkspaceIdMustBeString()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $workspaceId must be a string, boolean given');

        new WorkspaceItemSource('source-name', false, 'data-object', false);
    }

    public function testDataObjectMustBeString()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $dataObject must be a string, NULL given');

        new WorkspaceItemSource('source-name', 'workspace-id', null, false);
    }

    public function testIsSlicedMustBeBool()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $isSliced must be a boolean, NULL given');

        new WorkspaceItemSource('source-name', 'workspace-id', 'data-object', null);
    }
}
