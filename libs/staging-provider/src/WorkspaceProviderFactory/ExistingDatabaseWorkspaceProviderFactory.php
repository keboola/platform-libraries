<?php

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StorageApi\Workspaces;
use Keboola\StagingProvider\Exception\StagingProviderException;

class ExistingDatabaseWorkspaceProviderFactory extends AbstractCachedWorkspaceProviderFactory
{
    /** @var Workspaces */
    private $workspacesApiClient;

    /** @var string */
    private $workspaceId;

    /** @var string */
    private $workspacePassword;

    /**
     * @param Workspaces $workspacesApiClient
     * @param string $workspaceId
     * @param string $workspacePassword
     */
    public function __construct(Workspaces $workspacesApiClient, $workspaceId, $workspacePassword)
    {
        parent::__construct($workspacesApiClient);

        $this->workspacesApiClient = $workspacesApiClient;
        $this->workspaceId = $workspaceId;
        $this->workspacePassword = $workspacePassword;
    }

    protected function getWorkspaceData($workspaceClass)
    {
        $data = $this->workspacesApiClient->getWorkspace($this->workspaceId);
        $data['connection']['password'] = $this->workspacePassword;

        if ($data['connection']['backend'] !== $workspaceClass::getType()) {
            throw new StagingProviderException(sprintf(
                'Incompatible workspace type. Expected workspace backend is "%s", actual backend is "%s"',
                $workspaceClass::getType(),
                $data['connection']['backend']
            ));
        }

        return $data;
    }
}
