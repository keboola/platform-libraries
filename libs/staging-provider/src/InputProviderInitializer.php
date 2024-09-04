<?php

declare(strict_types=1);

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\Scope;

class InputProviderInitializer extends AbstractProviderInitializer
{
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
