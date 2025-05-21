<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

readonly class WorkspaceStaging implements WorkspaceStagingInterface
{
    public function __construct(
        private string $workspaceId,
    ) {
    }

    public function getWorkspaceId(): string
    {
        return $this->workspaceId;
    }
}
