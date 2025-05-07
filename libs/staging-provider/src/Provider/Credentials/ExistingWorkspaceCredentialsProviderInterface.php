<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Provider\Workspace;

interface ExistingWorkspaceCredentialsProviderInterface
{
    public function provideCredentials(Workspace $workspace): ?array;
}
