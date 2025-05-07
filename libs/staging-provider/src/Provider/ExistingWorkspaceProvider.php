<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\InputMapping\Staging\WorkspaceStagingInterface;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Credentials\ExistingWorkspaceCredentialsProviderInterface;
use Keboola\StorageApi\Workspaces;
use LogicException;

class ExistingWorkspaceProvider implements WorkspaceStagingInterface
{
    private ?Workspace $workspace = null;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly string $workspaceId,
        private readonly ExistingWorkspaceCredentialsProviderInterface $credentialsProvider,
    ) {
    }

    protected function getWorkspace(): Workspace
    {
        if ($this->workspace === null) {
            $workspaceData = $this->workspacesApiClient->getWorkspace((int) $this->workspaceId);
            $this->workspace = Workspace::createFromData($workspaceData);
        }

        return $this->workspace;
    }

    public function getWorkspaceId(): string
    {
        return $this->workspaceId;
    }

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

    public function getCredentials(): array
    {
        $workspace = $this->getWorkspace();

        if (!$workspace->hasCredentials()) {
            $credentials = $this->credentialsProvider->provideCredentials($workspace);
            $workspace->setCredentialsFromData($credentials);
        }

        if (!$workspace->hasCredentials()) {
            throw new LogicException('Credentials provider did not configure workspace credentials.');
        }

        return $workspace->getCredentials();
    }

    public function cleanup(): void
    {
        $this->workspacesApiClient->deleteWorkspace((int) $this->getWorkspaceId(), [], true);
    }
}
