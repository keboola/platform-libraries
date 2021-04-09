<?php

namespace Keboola\WorkspaceProvider\WorkspaceProviderFactory;

use Keboola\WorkspaceProvider\Provider\WorkspaceStagingProvider;
use Keboola\WorkspaceProvider\Staging\Workspace\WorkspaceStagingInterface;

interface WorkspaceProviderFactoryInterface
{
    /**
     * Return StagingProvider for given staging type.
     *
     * Same provider instance is returned across multiple calls with same staging type.
     *
     * @param class-string<WorkspaceStagingInterface> $stagingClass
     * @return WorkspaceStagingProvider
     */
    public function getProvider($stagingClass);
}
