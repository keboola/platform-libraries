<?php

declare(strict_types=1);

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\Scope;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StagingProvider\Provider\WorkspaceProviderInterface;

abstract class AbstractProviderInitializer
{
    public function __construct(
        private readonly AbstractStrategyFactory $stagingFactory,
        private readonly WorkspaceProviderInterface $workspaceStagingProvider,
        private readonly LocalStagingProvider $localStagingProvider,
    ) {
    }

    abstract public function initializeProviders(string $stagingType, array $tokenInfo): void;

    /**
     * @param array<string, Scope> $scopes
     */
    protected function addWorkspaceProvider(array $scopes): void
    {
        $this->stagingFactory->addProvider($this->workspaceStagingProvider, $scopes);
    }

    /**
     * @param array<string, Scope> $scopes
     */
    protected function addLocalProvider(array $scopes): void
    {
        $this->stagingFactory->addProvider($this->localStagingProvider, $scopes);
    }

    protected function addWorkspaceProviders(string $stagingType, array $tokenInfo): void
    {
        if ($stagingType === AbstractStrategyFactory::WORKSPACE_REDSHIFT &&
            $tokenInfo['owner']['hasRedshift']
        ) {
            $this->addWorkspaceProvider(
                [
                    AbstractStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::TABLE_DATA]),
                ],
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_SNOWFLAKE &&
            $tokenInfo['owner']['hasSnowflake']
        ) {
            $this->addWorkspaceProvider(
                [
                    AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::TABLE_DATA]),
                ],
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_SYNAPSE &&
            $tokenInfo['owner']['hasSynapse']
        ) {
            $this->addWorkspaceProvider(
                [
                    AbstractStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::TABLE_DATA]),
                ],
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_ABS &&
            $tokenInfo['owner']['fileStorageProvider'] === 'azure'
        ) {
            $this->addWorkspaceProvider(
                [
                    AbstractStrategyFactory::WORKSPACE_ABS => new Scope(
                        [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA],
                    ),
                ],
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_EXASOL &&
            $tokenInfo['owner']['hasExasol']
        ) {
            $this->addWorkspaceProvider(
                [
                    AbstractStrategyFactory::WORKSPACE_EXASOL => new Scope([Scope::TABLE_DATA]),
                ],
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_TERADATA &&
            $tokenInfo['owner']['hasTeradata']
        ) {
            $this->addWorkspaceProvider(
                [
                    AbstractStrategyFactory::WORKSPACE_TERADATA => new Scope([Scope::TABLE_DATA]),
                ],
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_BIGQUERY &&
            $tokenInfo['owner']['hasBigquery']
        ) {
            $this->addWorkspaceProvider(
                [
                    AbstractStrategyFactory::WORKSPACE_BIGQUERY => new Scope([Scope::TABLE_DATA]),
                ],
            );
        }
    }
}
