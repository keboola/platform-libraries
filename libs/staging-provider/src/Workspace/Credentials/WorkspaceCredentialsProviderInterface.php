<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Credentials;

use Keboola\StagingProvider\Workspace\WorkspaceInterface;

interface WorkspaceCredentialsProviderInterface
{
    public function provideCredentials(WorkspaceInterface $workspace): array;
}
