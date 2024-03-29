<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StagingProvider\WorkspaceProviderFactory\Credentials\CredentialsInterface;
use Keboola\StorageApi\Workspaces;

class ExistingFilesystemWorkspaceProviderFactory extends AbstractCachedWorkspaceProviderFactory
{
    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly string $workspaceId,
        private readonly CredentialsInterface $credentials,
    ) {
        parent::__construct($workspacesApiClient);
    }

    /**
     * @param class-string<WorkspaceStagingInterface> $workspaceClass
     * @return array
     */
    protected function getWorkspaceData(string $workspaceClass): array
    {
        $data = $this->workspacesApiClient->getWorkspace((int) $this->workspaceId);
        $data['connection'] = array_merge($data['connection'], $this->credentials->toArray());

        if ($data['connection']['backend'] !== $workspaceClass::getType()) {
            throw new StagingProviderException(sprintf(
                'Incompatible workspace type. Expected workspace backend is "%s", actual backend is "%s"',
                $workspaceClass::getType(),
                $data['connection']['backend'],
            ));
        }

        return $data;
    }
}
