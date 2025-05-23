<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

use Keboola\StorageApi\WorkspaceLoginType;

interface WorkspaceInterface
{
    public function getWorkspaceId(): string;

    public function getBackendType(): string;

    public function getBackendSize(): ?string;

    public function getLoginType(): WorkspaceLoginType;
}
