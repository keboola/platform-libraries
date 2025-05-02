<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Provider\Credentials\ExistingWorkspaceCredentialsProviderInterface;
use Keboola\StorageApi\Workspaces;
use LogicException;

class ExistingWorkspaceProvider extends BaseWorkspaceProvider
{
    private ?Workspace $workspace = null;

    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly string $workspaceId,
        private readonly ExistingWorkspaceCredentialsProviderInterface $credentialsProvider,
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
            $credentials = $this->credentialsProvider->provideCredentials($workspace);
            $workspace->setCredentialsFromData($credentials);
        }

        if (!$workspace->hasCredentials()) {
            throw new LogicException('Credentials provider did not configure workspace credentials.');
        }

        return $workspace->getCredentials();
    }
}
