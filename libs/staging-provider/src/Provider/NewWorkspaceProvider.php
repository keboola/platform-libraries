<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;

class NewWorkspaceProvider extends BaseWorkspaceProvider
{
    private ?Workspace $workspace = null;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly Components $componentsApiClient,
        private readonly SnowflakeKeypairGenerator $snowflakeKeypairGenerator,
        private readonly WorkspaceBackendConfig $workspaceBackendConfig,
        private readonly string $componentId,
        private readonly ?string $configId = null,
    ) {
        parent::__construct($workspacesApiClient);
    }

    protected function getWorkspace(): Workspace
    {
        if ($this->workspace !== null) {
            return $this->workspace;
        }

        $defaultLoginType = match ($this->workspaceBackendConfig->getStagingType()) {
// TODO enable once key-pair auth is default for Snowflake
//            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            default => WorkspaceLoginType::DEFAULT,
        };
        $loginType = $this->workspaceBackendConfig->getLoginType() ?? $defaultLoginType;

        $options = [
            'backend' => $this->workspaceBackendConfig->getStorageApiWorkspaceType(),
            'networkPolicy' => $this->workspaceBackendConfig->getNetworkPolicy(),
            'loginType' => $loginType,
        ];

        if ($this->workspaceBackendConfig->getStorageApiWorkspaceSize() !== null) {
            $options['backendSize'] = $this->workspaceBackendConfig->getStorageApiWorkspaceSize();
        }

        if ($this->workspaceBackendConfig->getUseReadonlyRole() !== null) {
            $options['readOnlyStorageAccess'] = $this->workspaceBackendConfig->getUseReadonlyRole();
        }

        $privateKey = null;
        if ($loginType->isKeyPairLogin()) {
            $keypair = $this->snowflakeKeypairGenerator->generateKeyPair();
            $options['publicKey'] = $keypair->publicKey;
            $privateKey = $keypair->privateKey;
        }

        if ($this->configId !== null) {
            // workspace tied to a component and configuration
            $workspaceData = $this->componentsApiClient->createConfigurationWorkspace(
                $this->componentId,
                $this->configId,
                $options,
                true,
            );
        } else {
            // workspace without associated configuration (workspace result is same, it's just different API call)
            $workspaceData = $this->workspacesApiClient->createWorkspace($options, true);
        }

        if ($privateKey !== null) {
            $workspaceData['connection']['privateKey'] = $privateKey;
        }

        $this->workspace = Workspace::createFromData($workspaceData);
        $this->workspace->setCredentialsFromData($workspaceData['connection']);

        return $this->workspace;
    }

    public function getWorkspaceId(): string
    {
        return $this->getWorkspace()->getId();
    }

    public function getCredentials(): array
    {
        return $this->getWorkspace()->getCredentials();
    }

    public function cleanup(): void
    {
        // only cleanup if a workspace was created yet
        if ($this->workspace === null) {
            return;
        }

        parent::cleanup();
    }
}
