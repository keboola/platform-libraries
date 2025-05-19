<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Workspace\ProviderConfig\WorkspaceConfigInterface;
use Keboola\StagingProvider\Workspace\WorkspaceInterface;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;

class LazyWorkspaceStaging implements WorkspaceStagingInterface
{
    use WorkspaceStagingTrait;

    private ?WorkspaceInterface $workspace = null;

    public function __construct(
        private readonly WorkspaceProvider $workspaceProvider,
        private readonly WorkspaceConfigInterface $workspaceProviderConfig,
    ) {
    }

    public function isInitialized(): bool
    {
        return $this->workspace !== null;
    }

    public function getWorkspace(): WorkspaceInterface
    {
        if ($this->workspace === null) {
            $this->workspace = $this->workspaceProvider->getWorkspace($this->workspaceProviderConfig);
        }

        return $this->workspace;
    }
}
