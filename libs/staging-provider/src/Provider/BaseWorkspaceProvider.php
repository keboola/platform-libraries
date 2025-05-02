<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
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
}
