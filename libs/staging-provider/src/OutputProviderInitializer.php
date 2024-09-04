<?php

declare(strict_types=1);

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;

class OutputProviderInitializer extends AbstractProviderInitializer
{
    public function __construct(
        OutputStrategyFactory $stagingFactory, // This is just to make sure the correct StrategyFactory is injected
        private readonly ProviderInterface $workspaceStagingProvider,
        private readonly ProviderInterface $localStagingProvider,
    ) {
        parent::__construct(
            $stagingFactory,
            $this->workspaceStagingProvider,
            $this->localStagingProvider,
        );
    }

    public function initializeProviders(string $stagingType, array $tokenInfo): void
    {
        $this->addWorkspaceProviders($stagingType, $tokenInfo);

        $this->addLocalProvider(
            [
                AbstractStrategyFactory::LOCAL => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::WORKSPACE_REDSHIFT => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::WORKSPACE_SYNAPSE => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::WORKSPACE_ABS => new Scope(
                    [Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::WORKSPACE_EXASOL => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::WORKSPACE_TERADATA => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::WORKSPACE_BIGQUERY => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA],
                ),
            ],
        );
    }
}
