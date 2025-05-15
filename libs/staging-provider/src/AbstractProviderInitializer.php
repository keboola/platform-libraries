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
