<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StagingProvider\Provider\WorkspaceStagingProvider;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;

interface WorkspaceProviderFactoryInterface
{
    /**
     * Return StagingProvider for the given staging type.
     *
     * Same provider instance is returned across multiple calls with same staging type.
     *
     * @param class-string<WorkspaceStagingInterface> $stagingClass
     */
    public function getProvider(string $stagingClass): WorkspaceStagingProvider;
}
