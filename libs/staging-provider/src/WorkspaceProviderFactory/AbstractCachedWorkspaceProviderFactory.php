<?php

namespace Keboola\WorkspaceProvider\WorkspaceProviderFactory;

use Keboola\StorageApi\Workspaces;
use Keboola\WorkspaceProvider\Provider\WorkspaceStagingProvider;
use Keboola\WorkspaceProvider\Staging\Workspace\WorkspaceStagingInterface;

abstract class AbstractCachedWorkspaceProviderFactory implements WorkspaceProviderFactoryInterface
{
    /** @var Workspaces */
    private $workspacesApiClient;

    /** @var array<class-string<WorkspaceStagingInterface>, WorkspaceStagingProvider> */
    private $providers;

    public function __construct(Workspaces $workspacesApiClient)
    {
        $this->workspacesApiClient = $workspacesApiClient;
    }

    public function getProvider($stagingClass)
    {
        if (isset($this->providers[$stagingClass])) {
            return $this->providers[$stagingClass];
        }

        return $this->providers[$stagingClass] = new WorkspaceStagingProvider(
            $this->workspacesApiClient,
            function () use ($stagingClass) {
                return new $stagingClass($this->getWorkspaceData($stagingClass));
            }
        );
    }

    /**
     * @param class-string<WorkspaceStagingInterface> $workspaceClass
     * @return array
     */
    abstract protected function getWorkspaceData($workspaceClass);
}
