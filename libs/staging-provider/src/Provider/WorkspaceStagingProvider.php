<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApi\Workspaces;

/**
 * @extends AbstractStagingProvider<WorkspaceStagingInterface>
 */
class WorkspaceStagingProvider extends AbstractStagingProvider
{
    private Workspaces $workspacesApiClient;

    /**
     * @param Workspaces $workspacesApiClient
     * @param callable(): WorkspaceStagingInterface $stagingGetter
     */
    public function __construct(Workspaces $workspacesApiClient, callable $stagingGetter)
    {
        parent::__construct($stagingGetter);

        $this->workspacesApiClient = $workspacesApiClient;
    }

    public function getWorkspaceId(): string
    {
        return $this->getStaging()->getWorkspaceId();
    }

    public function getCredentials(): array
    {
        return $this->getStaging()->getCredentials();
    }

    public function getPath(): string
    {
        throw new StagingProviderException('Workspace staging provider does not support path.');
    }

    public function cleanup(): void
    {
        $this->workspacesApiClient->deleteWorkspace($this->getWorkspaceId(), [], true);
    }

    public function getBackendSize(): ?string
    {
        return $this->getStaging()->getBackendSize();
    }
}
