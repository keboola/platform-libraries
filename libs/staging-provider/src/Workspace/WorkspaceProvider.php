<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

use InvalidArgumentException;
use Keboola\StagingProvider\Workspace\Configuration\WorkspaceCredentials;
use Keboola\StagingProvider\Workspace\Credentials\CredentialsProvider;
use Keboola\StagingProvider\Workspace\ProviderConfig\ExistingWorkspaceConfig;
use Keboola\StagingProvider\Workspace\ProviderConfig\NewWorkspaceConfig;
use Keboola\StagingProvider\Workspace\ProviderConfig\WorkspaceConfigInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;

class WorkspaceProvider
{
    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly Components $componentsApiClient,
        private readonly SnowflakeKeypairGenerator $snowflakeKeypairGenerator,
    ) {
    }

    public function getWorkspace(WorkspaceConfigInterface $config): Workspace
    {
        return match (true) {
            $config instanceof NewWorkspaceConfig => $this->createNewWorkspace($config),
            $config instanceof ExistingWorkspaceConfig => $this->getExistingWorkspace($config),
            default => throw new InvalidArgumentException(sprintf('Unsupported config type "%s"', $config::class)),
        };
    }

    public function cleanupWorkspace(string $workspaceId): void
    {
        try {
            $this->workspacesApiClient->deleteWorkspace((int) $workspaceId, [], true);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }

            // workspace does not exist, nothing to clean up
        }
    }

    private function createNewWorkspace(NewWorkspaceConfig $config): Workspace
    {
        $defaultLoginType = match ($config->stagingType) {
// TODO enable once key-pair auth is default for Snowflake
//            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            default => WorkspaceLoginType::DEFAULT,
        };
        $loginType = $config->loginType ?? $defaultLoginType;

        $options = [
            'backend' => $config->getStorageApiWorkspaceType(),
            'networkPolicy' => $config->networkPolicy->value,
            'loginType' => $loginType,
        ];

        // temporary workaround until https://github.com/keboola/connection/pull/5714 is released
        if ($options['loginType'] === WorkspaceLoginType::DEFAULT) {
            unset($options['loginType']);
        }

        if ($config->size !== null) {
            $options['backendSize'] = $config->size;
        }

        if ($config->useReadonlyRole !== null) {
            $options['readOnlyStorageAccess'] = $config->useReadonlyRole;
        }

        $privateKey = null;
        if ($loginType->isKeyPairLogin()) {
            $keypair = $this->snowflakeKeypairGenerator->generateKeyPair();
            $options['publicKey'] = $keypair->publicKey;
            $privateKey = $keypair->privateKey;
        }

        if ($config->configId !== null) {
            // workspace tied to a component and configuration
            $workspaceData = $this->componentsApiClient->createConfigurationWorkspace(
                $config->componentId,
                $config->configId,
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

        return Workspace::createFromData(
            new CredentialsProvider(new WorkspaceCredentials($workspaceData['connection'])),
            $workspaceData,
        );
    }

    private function getExistingWorkspace(ExistingWorkspaceConfig $config): Workspace
    {
        $workspaceData = $this->workspacesApiClient->getWorkspace((int) $config->workspaceId);

        return Workspace::createFromData(
            $config->credentials,
            $workspaceData,
        );
    }
}
