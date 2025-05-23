<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Staging\Workspace;

use Keboola\StagingProvider\Staging\Workspace\WorkspaceStaging;
use PHPUnit\Framework\TestCase;

class WorkspaceStagingTest extends TestCase
{
    public function testGetWorkspaceId(): void
    {
        $staging = new WorkspaceStaging('id');

        self::assertSame('id', $staging->getWorkspaceId());
    }
}
