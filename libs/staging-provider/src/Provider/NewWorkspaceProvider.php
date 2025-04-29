<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;

class NewWorkspaceProvider implements WorkspaceProviderInterface
{
    private ?StorageApiWorkspace $workspace = null;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly Components $componentsApiClient,
        private readonly WorkspaceBackendConfig $workspaceBackendConfig,
        private readonly string $componentId,
        private readonly ?string $configId = null,
    ) {
    }

    private function getWorkspace(): StorageApiWorkspace
    {
        if ($this->workspace !== null) {
            return $this->workspace;
        }

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
        if ($this->workspaceBackendConfig->getLoginType() !== null) {
            $options['loginType'] = $this->workspaceBackendConfig->getLoginType();
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

        return $this->workspace = StorageApiWorkspace::fromDataArray($data);
    }

    public function getWorkspaceId(): string
    {
        return $this->getWorkspace()->id;
    }

    public function getCredentials(): array
    {
        return $this->getWorkspace()->credentials;
    }

    public function getPath(): string
    {
        throw new StagingProviderException('Workspace staging provider does not support path.');
    }

    public function getBackendSize(): ?string
    {
        return $this->getWorkspace()->backendSize;
    }

    public function getBackendType(): string
    {
        return $this->getWorkspace()->backend;
    }

    public function cleanup(): void
    {
        // only cleanup if a workspace was created before
        if ($this->workspace === null) {
            return;
        }

        $this->workspacesApiClient->deleteWorkspace((int) $this->getWorkspaceId(), [], true);
    }
}
