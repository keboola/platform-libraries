<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Provider\ExistingWorkspaceProvider;
use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Provider\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;

class ResetCredentialsProvider implements ExistingWorkspaceCredentialsProviderInterface
{
    public function __construct(
        private readonly SnowflakeKeypairGenerator $snowflakeKeypairGenerator,
    ) {
    }

    public function provideCredentials(ExistingWorkspaceProvider $workspaceProvider, Workspace $workspace): void
    {
        if (in_array($workspace->getLoginType(), [
            WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
        ], true)) {
            $keyPair = $this->snowflakeKeypairGenerator->generateKeyPair();

            $workspaceProvider->resetCredentials([
                'publicKey' => $keyPair->publicKey,
            ]);
            $workspace->setCredentialsFromData([
                'privateKey' => $keyPair->privateKey,
            ]);
        } else {
            $workspaceProvider->resetCredentials([]);
        }
    }
}
