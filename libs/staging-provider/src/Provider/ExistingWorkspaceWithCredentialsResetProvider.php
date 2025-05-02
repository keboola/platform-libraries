<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;

class ExistingWorkspaceWithCredentialsResetProvider implements WorkspaceProviderInterface
{
    private ?StorageApiWorkspace $workspace = null;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly SnowflakeKeypairGenerator $snowflakeKeypairGenerator,
        private readonly string $workspaceId,
    ) {
    }

    private function getWorkspace(): StorageApiWorkspace
    {
        if ($this->workspace !== null) {
            return $this->workspace;
        }

        $workspaceData = $this->workspacesApiClient->getWorkspace((int) $this->workspaceId);

        $workspaceLoginType = WorkspaceLoginType::from(
            $workspaceData['connection']['loginType'] ?? WorkspaceLoginType::DEFAULT->value,
        );
        switch ($workspaceLoginType) {
            // key-pair auth
            case WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR:
            case WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR:
                $keyPair = $this->snowflakeKeypairGenerator->generateKeyPair();

                $credentials = [
                    'privateKey' => $keyPair->privateKey,
                ];

                // TODO finish once Connection endpoint is implemented
                // it's expected the endpoint will require just workspaceId + publicKey and response is not useful
                // as credentials (privateKey) are generated locally
                throw new StagingProviderException('Credentials reset for key-pair auth is not supported yet');

            // password-based auth
            case WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD:
                $credentials = $this->workspacesApiClient->resetWorkspacePassword((int) $this->workspaceId);
                break;

            // no credentials
            default:
                $credentials = [];
                break;
        }

        // getWorkspace returns workspace without credentials, these are supplied from the outside and merged
        $workspaceData['connection'] = array_merge(
            $workspaceData['connection'],
            $credentials,
        );
        return $this->workspace = StorageApiWorkspace::fromDataArray($workspaceData);
    }

    public function getWorkspaceId(): string
    {
        return $this->workspaceId;
    }

    public function getCredentials(): array
    {
        return $this->getWorkspace()->credentials;
    }

    public function getBackendSize(): ?string
    {
        return $this->getWorkspace()->backendSize;
    }

    public function getBackendType(): string
    {
        return $this->getWorkspace()->backend;
    }

    public function getPath(): string
    {
        throw new StagingProviderException(sprintf(
            '%s does not support path',
            self::class,
        ));
    }

    public function cleanup(): void
    {
        $this->workspacesApiClient->deleteWorkspace((int) $this->getWorkspaceId(), [], true);
    }
}
