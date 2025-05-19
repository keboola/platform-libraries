<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\ProviderConfig;

use Keboola\StagingProvider\Workspace\Credentials\WorkspaceCredentialsProviderInterface;

readonly class ExistingWorkspaceConfig implements WorkspaceConfigInterface
{
    public function __construct(
        public string $workspaceId,
        public WorkspaceCredentialsProviderInterface $credentials,
    ) {
    }
}
