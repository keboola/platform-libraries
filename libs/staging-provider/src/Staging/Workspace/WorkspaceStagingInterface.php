<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\Workspace;

use Keboola\StagingProvider\Workspace\WorkspaceInterface;

interface WorkspaceStagingInterface extends WorkspaceInterface
{
    public function cleanup(): void;
}
