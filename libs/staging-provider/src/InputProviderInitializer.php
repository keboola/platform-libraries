<?php

declare(strict_types=1);

namespace Keboola\StagingProvider;

use Keboola\InputMapping\File\StrategyInterface as FileStrategyInterface;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\FileStagingInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\InputMapping\Staging\WorkspaceStagingInterface;
use Keboola\InputMapping\Table\StrategyInterface as TableStrategyInterface;

/**
 * @extends AbstractProviderInitializer<TableStrategyInterface, FileStrategyInterface>
 */
class InputProviderInitializer extends AbstractProviderInitializer
{
    public function __construct(
        InputStrategyFactory $stagingFactory, // This is just to make sure the correct StrategyFactory is injected
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
                // TABLE_DATA for ABS and S3 is bound to LocalProvider because it requires no provider at all
                AbstractStrategyFactory::S3 => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA],
                ),
                AbstractStrategyFactory::ABS => new Scope(
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
