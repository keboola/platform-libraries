<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceCredentials;
use Keboola\StorageApi\Workspaces;

class ExistingWorkspaceWithExistingCredentialsProvider implements WorkspaceProviderInterface
{
    private ?StorageApiWorkspace $workspace = null;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly string $workspaceId,
        private readonly WorkspaceCredentials $userProvidedCredentials,
    ) {
    }

    private function getWorkspace(): StorageApiWorkspace
    {
        if ($this->workspace !== null) {
            return $this->workspace;
        }

        $workspaceData = $this->workspacesApiClient->getWorkspace((int) $this->workspaceId);

        $workspaceData['connection'] = array_merge(
            $workspaceData['connection'],
            $this->userProvidedCredentials->credentials,
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
