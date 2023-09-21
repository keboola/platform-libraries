<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StagingProvider\WorkspaceProviderFactory\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;

class ComponentWorkspaceProviderFactory extends AbstractCachedWorkspaceProviderFactory
{
    public function __construct(
        private readonly Components $componentsApiClient,
        private readonly Workspaces $workspacesApiClient,
        private readonly string $componentId,
        private readonly ?string $configId,
        private readonly WorkspaceBackendConfig $workspaceBackendConfig,
        private readonly ?bool $useWorkspaceWithReadonlyRole = null,
    ) {
        parent::__construct($workspacesApiClient);
    }

    /**
     * @param class-string<WorkspaceStagingInterface> $workspaceClass
     * @return array
     */
    protected function getWorkspaceData(string $workspaceClass): array
    {
        $options = [
            'backend' => $workspaceClass::getType(),
        ];

        $requestedBackendType = $this->workspaceBackendConfig->getType();
        if ($requestedBackendType !== null) {
            $options['backendSize'] = $requestedBackendType;
        }

        if ($this->useWorkspaceWithReadonlyRole !== null) {
            $options['readOnlyStorageAccess'] = $this->useWorkspaceWithReadonlyRole;
        }

        if ($this->configId) {
            return $this->componentsApiClient->createConfigurationWorkspace(
                $this->componentId,
                $this->configId,
                $options,
                true,
            );
        }

        return $this->workspacesApiClient->createWorkspace($options, true);
    }
}
