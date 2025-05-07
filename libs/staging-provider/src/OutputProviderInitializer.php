<?php

declare(strict_types=1);

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\FileStagingInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\WorkspaceStagingInterface;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\Writer\File\StrategyInterface as FileStrategyInterface;
use Keboola\OutputMapping\Writer\Table\StrategyInterface as TableStrategyInterface;

/**
 * @extends AbstractProviderInitializer<TableStrategyInterface, FileStrategyInterface>
 */
class OutputProviderInitializer extends AbstractProviderInitializer
{
    public function __construct(
        OutputStrategyFactory $stagingFactory, // This is just to make sure the correct StrategyFactory is injected
        private readonly WorkspaceStagingInterface $workspaceStaging,
        private readonly FileStagingInterface $localStaging,
    ) {
        parent::__construct(
            $stagingFactory,
            $this->workspaceStaging,
            $this->localStaging,
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
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::WORKSPACE_BIGQUERY => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA],
                ),
            ],
        );
    }
}
