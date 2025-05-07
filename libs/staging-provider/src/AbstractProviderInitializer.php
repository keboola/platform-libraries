<?php

declare(strict_types=1);

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\FileStagingInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\WorkspaceStagingInterface;

/**
 * @template T_TABLE_STAGING of object
 * @template T_FILE_STAGING of object
 */
abstract class AbstractProviderInitializer
{
    /**
     * @param AbstractStrategyFactory<T_TABLE_STAGING, T_FILE_STAGING> $stagingFactory
     */
    public function __construct(
        private readonly AbstractStrategyFactory $stagingFactory,
        private readonly WorkspaceStagingInterface $workspaceStaging,
        private readonly FileStagingInterface $localStaging,
    ) {
    }

    abstract public function initializeProviders(string $stagingType, array $tokenInfo): void;

    /**
     * @param array<string, Scope> $scopes
     */
    protected function addWorkspaceProvider(array $scopes): void
    {
        $this->stagingFactory->addProvider($this->workspaceStaging, $scopes);
    }

    /**
     * @param array<string, Scope> $scopes
     */
    protected function addLocalProvider(array $scopes): void
    {
        $this->stagingFactory->addProvider($this->localStaging, $scopes);
    }

    protected function addWorkspaceProviders(string $stagingType, array $tokenInfo): void
    {
        if ($stagingType === AbstractStrategyFactory::WORKSPACE_SNOWFLAKE &&
            $tokenInfo['owner']['hasSnowflake']
        ) {
            $this->addWorkspaceProvider(
                [
                    AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::TABLE_DATA]),
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
