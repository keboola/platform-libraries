<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Workspace\WorkspaceInterface;

class NoCredentialsProvider implements WorkspaceCredentialsProviderInterface
{
    public function provideCredentials(WorkspaceInterface $workspace): array
    {
        throw new StagingProviderException('Credentials are not available');
    }
}
