<?php

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StagingProvider\Provider\WorkspaceStagingProvider;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;

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
