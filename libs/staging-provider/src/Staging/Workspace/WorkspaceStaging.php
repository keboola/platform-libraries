<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Workspace\WorkspaceInterface;

class WorkspaceStaging implements WorkspaceStagingInterface
{
    use WorkspaceStagingTrait;

    public function __construct(
        private readonly WorkspaceInterface $workspace,
    ) {
    }

    private function getWorkspace(): WorkspaceInterface
    {
        return $this->workspace;
    }
}
