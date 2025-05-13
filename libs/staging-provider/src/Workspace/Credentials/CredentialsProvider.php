<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Credentials;

use Keboola\StagingProvider\Workspace\Configuration\WorkspaceCredentials;
use Keboola\StagingProvider\Workspace\Workspace;

class CredentialsProvider implements WorkspaceCredentialsProviderInterface
{
    public function __construct(
        private readonly WorkspaceCredentials $userProvidedCredentials,
    ) {
    }

    public function provideCredentials(Workspace $workspace): array
    {
        return $this->userProvidedCredentials->credentials;
    }
}
