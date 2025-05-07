<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Workspace;

class NoCredentialsProvider implements ExistingWorkspaceCredentialsProviderInterface
{
    public function provideCredentials(Workspace $workspace): ?array
    {
        throw new StagingProviderException('Credentials are not available');
    }
}
