<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Source;

use Keboola\OutputMapping\Writer\Table\Source\SourceType;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use PHPUnit\Framework\TestCase;

class WorkspaceItemSourceTest extends TestCase
{
    public function testGetters(): void
    {
        $source = new WorkspaceItemSource('source-name', 'workspace-id', 'data-object', true);

        self::assertSame('source-name', $source->getName());
        self::assertSame('workspace-id', $source->getWorkspaceId());
        self::assertSame('data-object', $source->getDataObject());
        self::assertTrue($source->isSliced());
        self::assertSame(SourceType::WORKSPACE, $source->getSourceType());
    }
}
