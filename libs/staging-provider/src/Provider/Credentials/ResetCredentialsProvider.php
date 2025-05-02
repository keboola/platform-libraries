<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Credentials;

use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Provider\Workspace;
use Keboola\StagingProvider\Provider\WorkspaceCredentialsReset;
use Keboola\StorageApi\WorkspaceLoginType;

class ResetCredentialsProvider implements ExistingWorkspaceCredentialsProviderInterface
{
    public function __construct(
        private readonly WorkspaceCredentialsReset $credentialsReset,
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

            $this->credentialsReset->resetWorkspaceCredentials($workspace->getId(), [
                'publicKey' => $keyPair->publicKey,
            ]);

            return [
                'privateKey' => $keyPair->privateKey,
            ];
        } else {
            return $this->credentialsReset->resetWorkspaceCredentials($workspace->getId(), []);
        }
    }
}
