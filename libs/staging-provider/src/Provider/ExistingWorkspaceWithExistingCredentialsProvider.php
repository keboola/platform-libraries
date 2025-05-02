<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Provider\Configuration\WorkspaceCredentials;
use Keboola\StorageApi\Workspaces;

class ExistingWorkspaceWithExistingCredentialsProvider extends BaseExistingWorkspaceProvider
{
    public function __construct(
        Workspaces $workspacesApiClient,
        string $workspaceId,
        private readonly WorkspaceCredentials $userProvidedCredentials,
    ) {
        parent::__construct($workspacesApiClient, $workspaceId);
    }

    protected function provideCredentials(Workspace $workspace): void
    {
        $workspace->setCredentialsFromData($this->userProvidedCredentials->credentials);
    }
}
