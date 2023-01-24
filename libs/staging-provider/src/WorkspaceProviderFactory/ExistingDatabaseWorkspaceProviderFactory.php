<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApi\Workspaces;

class ExistingDatabaseWorkspaceProviderFactory extends AbstractCachedWorkspaceProviderFactory
{
    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly string $workspaceId,
        private readonly string $workspacePassword
    ) {
        parent::__construct($workspacesApiClient);
    }

    /**
     * @param class-string<WorkspaceStagingInterface> $workspaceClass
     */
    protected function getWorkspaceData(string $workspaceClass): array
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
