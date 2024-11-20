<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;

class NewWorkspaceStagingProvider extends AbstractWorkspaceProvider
{
    private StorageApiWorkspace $workspace;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly Components $componentsApiClient,
        private readonly WorkspaceBackendConfig $workspaceBackendConfig,
        private readonly string $componentId,
        private readonly ?string $configId = null,
    ) {
    }

    protected function getWorkspace(): StorageApiWorkspace
    {
        if (!isset($this->workspace)) {
            $options = [
                'backend' => $this->workspaceBackendConfig->getStorageApiWorkspaceType(),
                'networkPolicy' => $this->workspaceBackendConfig->getNetworkPolicy(),
            ];
            if ($this->workspaceBackendConfig->getStorageApiWorkspaceSize() !== null) {
                $options['backendSize'] = $this->workspaceBackendConfig->getStorageApiWorkspaceSize();
            }
            if ($this->workspaceBackendConfig->getUseReadonlyRole() !== null) {
                $options['readOnlyStorageAccess'] = $this->workspaceBackendConfig->getUseReadonlyRole();
            }

            if ($this->configId !== null) {
                // workspace tied to a component and configuration
                $data = $this->componentsApiClient->createConfigurationWorkspace(
                    $this->componentId,
                    $this->configId,
                    $options,
                    true,
                );
            } else {
                // workspace without associated configuration (workspace result is same, it's just different API call)
                $data = $this->workspacesApiClient->createWorkspace($options, true);
            }
            $this->workspace = StorageApiWorkspace::fromDataArray($data);
        }
        return $this->workspace;
    }

    public function cleanup(): void
    {
        if (isset($this->workspace)) {
            $this->workspacesApiClient->deleteWorkspace((int) $this->getWorkspaceId(), [], true);
        }
    }
}
