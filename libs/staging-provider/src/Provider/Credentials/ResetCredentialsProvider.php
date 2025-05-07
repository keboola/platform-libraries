<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Provider\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;

class ResetCredentialsProvider implements ExistingWorkspaceCredentialsProviderInterface
{
    public function __construct(
        private readonly Workspaces $workspacesApiClient,
        private readonly SnowflakeKeypairGenerator $snowflakeKeypairGenerator,
    ) {
    }

    public function provideCredentials(Workspace $workspace): ?array
    {
        if (in_array($workspace->getLoginType(), [
            WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
        ], true)) {
            $keyPair = $this->snowflakeKeypairGenerator->generateKeyPair();

            $this->workspacesApiClient->resetCredentials(
                $workspace->getId(),
                new Workspaces\ResetCredentialsRequest(
                    publicKey: $keyPair->publicKey,
                ),
            );

            return [
                'privateKey' => $keyPair->privateKey,
            ];
        } else {
            return $this->workspacesApiClient->resetCredentials(
                $workspace->getId(),
                new Workspaces\ResetCredentialsRequest(),
            );
        }
    }
}
