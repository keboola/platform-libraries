<?php

namespace Keboola\WorkspaceProvider\WorkspaceProviderFactory;

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

    public function __construct(
        Components $componentsApiClient,
        Workspaces $workspacesApiClient,
        $componentId,
        $configId
    ) {
        parent::__construct($workspacesApiClient);

        $this->componentsApiClient = $componentsApiClient;
        $this->workspacesApiClient = $workspacesApiClient;
        $this->componentId = $componentId;
        $this->configId = $configId;
    }

    protected function getWorkspaceData($workspaceClass)
    {
        $options = ['backend' => $workspaceClass::getType()];

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
