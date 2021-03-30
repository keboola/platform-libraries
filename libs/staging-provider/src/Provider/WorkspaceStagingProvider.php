<?php

namespace Keboola\WorkspaceProvider\Provider;

use Keboola\StorageApi\Workspaces;
use Keboola\WorkspaceProvider\Exception\StagingProviderException;
use Keboola\WorkspaceProvider\Staging\Workspace\WorkspaceStagingInterface;

/**
 * @extends AbstractStagingProvider<WorkspaceStagingInterface>
 */
class WorkspaceStagingProvider extends AbstractStagingProvider
{
    /** @var Workspaces */
    private $workspacesApiClient;

    /**
     * @param Workspaces $workspacesApiClient
     * @param callable(): WorkspaceStagingInterface $stagingGetter
     */
    public function __construct(Workspaces $workspacesApiClient, callable $stagingGetter)
    {
        parent::__construct($stagingGetter, WorkspaceStagingInterface::class);

        $this->workspacesApiClient = $workspacesApiClient;
    }

    public function getWorkspaceId()
    {
        return $this->getStaging()->getWorkspaceId();
    }

    public function getCredentials()
    {
        return $this->getStaging()->getCredentials();
    }

    public function getPath()
    {
        throw new StagingProviderException('Workspace staging provider does not support path.');
    }

    public function cleanup()
    {
        $this->workspacesApiClient->deleteWorkspace($this->getWorkspaceId(), ['async' => true]);
    }
}
