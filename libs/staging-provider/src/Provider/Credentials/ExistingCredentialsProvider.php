<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Provider\Configuration\WorkspaceCredentials;
use Keboola\StagingProvider\Provider\ExistingWorkspaceProvider;
use Keboola\StagingProvider\Provider\Workspace;

class ExistingCredentialsProvider implements ExistingWorkspaceCredentialsProviderInterface
{
    public function __construct(
        private readonly WorkspaceCredentials $userProvidedCredentials,
    ) {
    }

    public function provideCredentials(ExistingWorkspaceProvider $workspaceProvider, Workspace $workspace): void
    {
        $workspace->setCredentialsFromData($this->userProvidedCredentials->credentials);
    }
}
