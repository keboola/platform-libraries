<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Workspace\ProviderConfig\WorkspaceConfigInterface;
use Keboola\StagingProvider\Workspace\Workspace;
use Keboola\StagingProvider\Workspace\WorkspaceInterface;
use Keboola\StagingProvider\Workspace\WorkspaceProvider;

class LazyWorkspaceStaging implements WorkspaceStagingInterface
{
    private ?Workspace $workspace = null;

    public function __construct(
        private readonly WorkspaceProvider $workspaceProvider,
        private readonly WorkspaceConfigInterface $workspaceProviderConfig,
    ) {
    }

    public function getWorkspaceId(): string
    {
        return $this->getWorkspace()->getWorkspaceId();
    }

    public function getBackendType(): string
    {
        return $this->getWorkspace()->getBackendType();
    }

    public function getBackendSize(): ?string
    {
        return $this->getWorkspace()->getBackendSize();
    }

    public function getCredentials(): array
    {
        return $this->getWorkspace()->getCredentials();
    }

    public function cleanup(): void
    {
        if ($this->workspace === null || $this->workspaceProviderConfig->isReusable()) {
            return;
        }

        $this->workspaceProvider->cleanupWorkspace($this->workspace->getWorkspaceId());
    }

    public function isInitialized(): bool
    {
        return $this->workspace !== null;
    }

    private function getWorkspace(): WorkspaceInterface
    {
        if ($this->workspace === null) {
            $this->workspace = $this->workspaceProvider->getWorkspace($this->workspaceProviderConfig);
        }

        return $this->workspace;
    }
}
