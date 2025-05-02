<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Provider\Configuration\WorkspaceCredentials;
use Keboola\StagingProvider\Provider\Workspace;

class ExistingCredentialsProvider implements ExistingWorkspaceCredentialsProviderInterface
{
    public function __construct(
        private readonly WorkspaceCredentials $userProvidedCredentials,
    ) {
    }

    public function provideCredentials(Workspace $workspace): ?array
    {
        return $this->userProvidedCredentials->credentials;
    }
}
