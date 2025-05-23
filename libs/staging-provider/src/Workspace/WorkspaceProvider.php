<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Workspace\Configuration\NewWorkspaceConfig;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\StorageApiToken;

class WorkspaceProvider
{
    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly Components $componentsApiClient,
        private readonly SnowflakeKeypairGenerator $snowflakeKeypairGenerator,
    ) {
    }

    public function createNewWorkspace(
        StorageApiToken $storageApiToken,
        NewWorkspaceConfig $config,
    ): WorkspaceWithCredentialsInterface {
        $stagingType = $config->stagingType;

        if ($stagingType->getStagingClass() !== StagingClass::Workspace) {
            throw new StagingProviderException(sprintf(
                'Can\'t create workspace for staging type "%s"',
                $stagingType->value,
            ));
        }

        $tokenOwnerInfo = $storageApiToken->getTokenInfo()['owner'] ?? [];
        if (!match ($stagingType) {
            StagingType::WorkspaceSnowflake => $tokenOwnerInfo['hasSnowflake'] ?? false,
            StagingType::WorkspaceBigquery => $tokenOwnerInfo['hasBigquery'] ?? false,
            default => false,
        }) {
            throw new StagingProviderException(sprintf(
                'The project does not support "%s" table backend.',
                $stagingType->value,
            ));
        }

        $defaultLoginType = match ($stagingType) {
// TODO enable once key-pair auth is default for Snowflake
//            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            default => WorkspaceLoginType::DEFAULT,
        };
        $loginType = $config->loginType ?? $defaultLoginType;

        $options = [
            'backend' => match ($stagingType) {
                StagingType::WorkspaceBigquery => 'bigquery',
                StagingType::WorkspaceSnowflake => 'snowflake',

                default => throw new StagingProviderException(sprintf(
                    'Unknown staging type "%s"',
                    $stagingType->value,
                )),
            },
            'networkPolicy' => $config->networkPolicy->value,
            'loginType' => $loginType,
        ];

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

        return WorkspaceWithCredentials::createFromData($workspaceData);
    }

    /**
     * @phpstan-return ($credentialsData is null ? WorkspaceInterface : WorkspaceWithCredentialsInterface)
     */
    public function getExistingWorkspace(string $workspaceId, ?array $credentialsData): WorkspaceInterface
    {
        $workspaceData = $this->workspacesApiClient->getWorkspace((int) $workspaceId);

        if ($credentialsData === null) {
            return Workspace::createFromData($workspaceData);
        }

        $workspaceData['connection'] = [
            ...$credentialsData,
            ...$workspaceData['connection'],
        ];

        return WorkspaceWithCredentials::createFromData($workspaceData);
    }

    public function resetWorkspaceCredentials(WorkspaceInterface $workspace): array
    {
        if ($workspace->getLoginType()->isKeyPairLogin()) {
            $keyPair = $this->snowflakeKeypairGenerator->generateKeyPair();

            $this->workspacesApiClient->resetCredentials(
                $workspace->getWorkspaceId(),
                new Workspaces\ResetCredentialsRequest(
                    publicKey: $keyPair->publicKey,
                ),
            );

            $credentials = [
                'privateKey' => $keyPair->privateKey,
            ];
        } else {
            $credentials = $this->workspacesApiClient->resetCredentials(
                $workspace->getWorkspaceId(),
                new Workspaces\ResetCredentialsRequest(),
            );
        }

        return $credentials;
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
}
