<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StagingProvider\Provider\WorkspaceStagingProvider;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApi\Workspaces;

abstract class AbstractCachedWorkspaceProviderFactory implements WorkspaceProviderFactoryInterface
{
    /** @var array<class-string<WorkspaceStagingInterface>, WorkspaceStagingProvider> */
    private array $providers;

    public function __construct(private readonly Workspaces $workspacesApiClient)
    {
    }

    public function getProvider($stagingClass): WorkspaceStagingProvider
    {
        if (isset($this->providers[$stagingClass])) {
            return $this->providers[$stagingClass];
        }

        return $this->providers[$stagingClass] = new WorkspaceStagingProvider(
            $this->workspacesApiClient,
            function () use ($stagingClass) {
                return new $stagingClass($this->getWorkspaceData($stagingClass));
            },
        );
    }

    /**
     * @param class-string<WorkspaceStagingInterface> $workspaceClass
     * @return array
     */
    abstract protected function getWorkspaceData(string $workspaceClass): array;
}
