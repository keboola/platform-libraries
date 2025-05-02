<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;

class ExistingWorkspaceWithCredentialsResetProvider extends BaseExistingWorkspaceProvider
{
    public function __construct(
        Workspaces $workspacesApiClient,
        private readonly SnowflakeKeypairGenerator $snowflakeKeypairGenerator,
        string $workspaceId,
    ) {
        parent::__construct($workspacesApiClient, $workspaceId);
    }

    protected function provideCredentials(Workspace $workspace): void
    {
        if (in_array($workspace->getLoginType(), [
            WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR,
            WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
        ], true)) {
            $keyPair = $this->snowflakeKeypairGenerator->generateKeyPair();

            $this->resetCredentials([
                'publicKey' => $keyPair->publicKey,
            ]);
            $workspace->setCredentialsFromData([
                'privateKey' => $keyPair->privateKey,
            ]);
        } else {
            $this->resetCredentials([]);
        }
    }
}
