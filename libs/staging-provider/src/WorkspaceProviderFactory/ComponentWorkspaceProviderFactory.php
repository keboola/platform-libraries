<?php

namespace Keboola\StagingProvider\WorkspaceProviderFactory;

use Keboola\StagingProvider\WorkspaceProviderFactory\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;

class ComponentWorkspaceProviderFactory extends AbstractCachedWorkspaceProviderFactory
{
    /** @var Components */
    private $componentsApiClient;

    /** @var Workspaces */
    private $workspacesApiClient;

    /** @var string */
    private $componentId;

    /** @var null|string */
    private $configId;

    /** @var WorkspaceBackendConfig */
    private $workspaceBackendConfig;

    public function __construct(
        Components $componentsApiClient,
        Workspaces $workspacesApiClient,
        $componentId,
        $configId,
        WorkspaceBackendConfig $workspaceBackendConfig
    ) {
        parent::__construct($workspacesApiClient);

        $this->componentsApiClient = $componentsApiClient;
        $this->workspacesApiClient = $workspacesApiClient;
        $this->componentId = $componentId;
        $this->configId = $configId;
        $this->workspaceBackendConfig = $workspaceBackendConfig;
    }

    protected function getWorkspaceData($workspaceClass)
    {
        $options = [
            'backend' => $workspaceClass::getType(),
        ];

        $requestedBackendType = $this->workspaceBackendConfig->getType();
        if ($requestedBackendType !== null) {
            $options['backendSize'] = $requestedBackendType;
        }

        if ($this->configId) {
            return $this->componentsApiClient->createConfigurationWorkspace(
                $this->componentId,
                $this->configId,
                $options
            );
        }

        return $this->workspacesApiClient->createWorkspace($options);
    }
}
