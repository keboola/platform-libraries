<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Workspace\WorkspaceInterface;
use Keboola\StorageApi\WorkspaceLoginType;

trait WorkspaceStagingTrait
{
    abstract private function getWorkspace(): WorkspaceInterface;

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

    public function getLoginType(): WorkspaceLoginType
    {
        return $this->getWorkspace()->getLoginType();
    }
}
