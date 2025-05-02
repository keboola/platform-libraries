<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;

abstract class BaseWorkspaceProvider implements WorkspaceProviderInterface
{
    public function __construct(
        private readonly Workspaces $workspacesApiClient,
    ) {
    }

    abstract protected function getWorkspace(): Workspace;

    public function getBackendSize(): ?string
    {
        return $this->getWorkspace()->getBackendSize();
    }

    public function getBackendType(): string
    {
        return $this->getWorkspace()->getBackendType();
    }

    public function getPath(): string
    {
        throw new StagingProviderException(sprintf(
            '%s does not support path',
            static::class,
        ));
    }

    public function cleanup(): void
    {
        $this->workspacesApiClient->deleteWorkspace((int) $this->getWorkspaceId(), [], true);
    }

    public function resetCredentials(array $params): void
    {
        $workspace = $this->getWorkspace();

        switch ($workspace->getLoginType()) {
            case WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR:
            case WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR:
                if (array_keys($params) !== ['publicKey']) {
                    throw new StagingProviderException('Invalid parameters for key-pair authentication');
                }

                // TODO pass public key to Storage API once API endpoint is ready

                // we don't know the private key, so we don't have complete credentials
                $credentialsData = null;
                break;

            default:
                $credentialsData = $this->workspacesApiClient->resetWorkspacePassword((int) $workspace->getId());
                break;
        }

        $workspace->setCredentialsFromData($credentialsData);
    }
}
