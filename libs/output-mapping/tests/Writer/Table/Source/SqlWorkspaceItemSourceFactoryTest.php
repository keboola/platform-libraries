<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Source;

use Keboola\OutputMapping\Writer\Table\Source\SqlWorkspaceItemSourceFactory;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use PHPUnit\Framework\TestCase;

class SqlWorkspaceItemSourceFactoryTest extends TestCase
{
    public function testCreateSource(): void
    {
        $stagingProviderMock = $this->createMock(WorkspaceStagingInterface::class);
        $stagingProviderMock->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn('123456')
        ;

        $factory = new SqlWorkspaceItemSourceFactory($stagingProviderMock);
        $workspaceItemSource = $factory->createSource('upload', 'myName');

        self::assertSame('123456', $workspaceItemSource->getWorkspaceId());
        self::assertSame('myName', $workspaceItemSource->getName());
        self::assertSame('myName', $workspaceItemSource->getDataObject());
        self::assertFalse($workspaceItemSource->isSliced());
    }
}
