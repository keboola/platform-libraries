<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;

class WorkspaceCredentialsReset
{
    public function __construct(
        private readonly Workspaces $workspacesApiClient,
    ) {
    }

    public function resetWorkspaceCredentials(string $workspaceId, array $params): array
    {
        $workspaceData = $this->workspacesApiClient->getWorkspace((int) $workspaceId);
        $loginType = WorkspaceLoginType::from(
            $workspaceData['connection']['loginType'] ?? WorkspaceLoginType::DEFAULT->value,
        );

        switch ($loginType) {
            case WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR:
            case WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR:
                if (array_keys($params) !== ['publicKey']) {
                    throw new StagingProviderException('Invalid parameters for key-pair authentication');
                }

                // TODO pass public key to Storage API once API endpoint is ready

                return [];

            default:
                return $this->workspacesApiClient->resetWorkspacePassword((int) $workspaceId);
        }
    }
}
