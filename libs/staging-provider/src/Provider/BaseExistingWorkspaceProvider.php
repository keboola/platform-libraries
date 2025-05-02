<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StorageApi\Workspaces;

abstract class BaseExistingWorkspaceProvider extends BaseWorkspaceProvider
{
    private ?Workspace $workspace = null;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly string $workspaceId,
    ) {
        parent::__construct($workspacesApiClient);
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

    public function getCredentials(): array
    {
        $workspace = $this->getWorkspace();

        if (!$workspace->hasCredentials()) {
            $this->provideCredentials($workspace);
        }

        return $workspace->getCredentials();
    }

    abstract protected function provideCredentials(Workspace $workspace): void;
}
