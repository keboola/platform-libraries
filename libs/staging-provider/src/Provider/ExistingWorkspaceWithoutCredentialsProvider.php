<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StorageApi\Workspaces;

class ExistingWorkspaceWithoutCredentialsProvider implements WorkspaceProviderInterface
{
    private ?StorageApiWorkspace $workspace = null;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly string $workspaceId,
    ) {
    }

    private function getWorkspace(): StorageApiWorkspace
    {
        if ($this->workspace !== null) {
            return $this->workspace;
        }

        $workspaceData = $this->workspacesApiClient->getWorkspace((int) $this->workspaceId);
        return $this->workspace = StorageApiWorkspace::fromDataArray($workspaceData);
    }

    public function getWorkspaceId(): string
    {
        return $this->workspaceId;
    }

    public function getBackendSize(): ?string
    {
        return $this->getWorkspace()->backendSize;
    }

    public function getBackendType(): string
    {
        return $this->getWorkspace()->backend;
    }

    public function getCredentials(): array
    {
        throw new StagingProviderException(sprintf(
            '%s does not support credentials',
            self::class,
        ));
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
