<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Provider\Credentials\CredentialsInterface;
use Keboola\StorageApi\Workspaces;

class ExistingWorkspaceStagingProvider extends AbstractWorkspaceProvider
{
    private StorageApiWorkspace $workspace;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly string $workspaceId,
        private readonly CredentialsInterface $credentials,
    ) {
    }

    protected function getWorkspace(): StorageApiWorkspace
    {
        if (!isset($this->workspace)) {
            // getWorkspace returns workspace without credentials, these are supplied from the outside and merged
            $data = $this->workspacesApiClient->getWorkspace((int) $this->workspaceId);
            $data['connection'] = array_merge($data['connection'], $this->credentials->toArray());
            $this->workspace = StorageApiWorkspace::fromDataArray($data);
        }
        return $this->workspace;
    }

    public function cleanup(): void
    {
        $this->workspacesApiClient->deleteWorkspace((int) $this->getWorkspaceId(), [], true);
    }
}
