<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Credentials;

use Keboola\StagingProvider\Workspace\Workspace;

interface WorkspaceCredentialsProviderInterface
{
    public function provideCredentials(Workspace $workspace): array;
}
