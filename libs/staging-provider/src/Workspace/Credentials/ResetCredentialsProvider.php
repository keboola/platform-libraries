<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Credentials;

use Keboola\StagingProvider\Workspace\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Workspace\WorkspaceInterface;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;

class ResetCredentialsProvider implements WorkspaceCredentialsProviderInterface
{
    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly SnowflakeKeypairGenerator $snowflakeKeypairGenerator,
    ) {
    }

    public function provideCredentials(WorkspaceInterface $workspace): array
    {
        if (in_array($workspace->getLoginType(), [
            WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
        ], true)) {
            $keyPair = $this->snowflakeKeypairGenerator->generateKeyPair();

            $this->workspacesApiClient->resetCredentials(
                $workspace->getWorkspaceId(),
                new Workspaces\ResetCredentialsRequest(
                    publicKey: $keyPair->publicKey,
                ),
            );

            return [
                'privateKey' => $keyPair->privateKey,
            ];
        } else {
            return $this->workspacesApiClient->resetCredentials(
                $workspace->getWorkspaceId(),
                new Workspaces\ResetCredentialsRequest(),
            );
        }
    }
}
