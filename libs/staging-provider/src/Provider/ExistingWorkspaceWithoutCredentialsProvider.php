<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;

class ExistingWorkspaceWithoutCredentialsProvider extends BaseExistingWorkspaceProvider
{
    protected function provideCredentials(Workspace $workspace): void
    {
        throw new StagingProviderException('Credentials are not available');
    }
}
