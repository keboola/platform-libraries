<?php

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StorageApi\Workspaces;
use Keboola\StagingProvider\Exception\StagingProviderException;

class ExistingFilesystemWorkspaceProviderFactory extends AbstractCachedWorkspaceProviderFactory
{
    /** @var Workspaces */
    private $workspacesApiClient;

    /** @var string */
    private $workspaceId;

    /** @var string */
    private $connectionString;

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
        $this->connectionString = $workspacePassword;
    }

    protected function getWorkspaceData($workspaceClass)
    {
        $data = $this->workspacesApiClient->getWorkspace($this->workspaceId);
        $data['connection']['connectionString'] = $this->connectionString;

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
