<?php

declare(strict_types=1);

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\Scope;
use Keboola\StagingProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\BigQueryWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\ExasolWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\RedshiftWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SynapseWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\TeradataWorkspaceStaging;

class InputProviderInitializer extends AbstractProviderInitializer
{
    public function initializeProviders(
        string $stagingType,
        array $tokenInfo
    ): void {
        if ($stagingType === AbstractStrategyFactory::WORKSPACE_REDSHIFT &&
            $tokenInfo['owner']['hasRedshift']
        ) {
            $this->addWorkspaceProvider(
                RedshiftWorkspaceStaging::class,
                [
                    AbstractStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_SNOWFLAKE &&
            $tokenInfo['owner']['hasSnowflake']
        ) {
            $this->addWorkspaceProvider(
                SnowflakeWorkspaceStaging::class,
                [
                    AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_SYNAPSE &&
            $tokenInfo['owner']['hasSynapse']
        ) {
            $this->addWorkspaceProvider(
                SynapseWorkspaceStaging::class,
                [
                    AbstractStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_ABS &&
            $tokenInfo['owner']['fileStorageProvider'] === 'azure'
        ) {
            $this->addWorkspaceProvider(
                AbsWorkspaceStaging::class,
                [
                    AbstractStrategyFactory::WORKSPACE_ABS => new Scope(
                        [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA]
                    ),
                ]
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_EXASOL &&
            $tokenInfo['owner']['hasExasol']
        ) {
            $this->addWorkspaceProvider(
                ExasolWorkspaceStaging::class,
                [
                    AbstractStrategyFactory::WORKSPACE_EXASOL => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_TERADATA &&
            $tokenInfo['owner']['hasTeradata']
        ) {
            $this->addWorkspaceProvider(
                TeradataWorkspaceStaging::class,
                [
                    AbstractStrategyFactory::WORKSPACE_TERADATA => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === AbstractStrategyFactory::WORKSPACE_BIGQUERY &&
            $tokenInfo['owner']['hasBigquery']
        ) {
            $this->addWorkspaceProvider(
                BigQueryWorkspaceStaging::class,
                [
                    AbstractStrategyFactory::WORKSPACE_BIGQUERY => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        $this->addLocalProvider(
            [
                AbstractStrategyFactory::LOCAL => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]
                ),
                // TABLE_DATA for ABS and S3 is bound to LocalProvider because it requires no provider at all
                AbstractStrategyFactory::S3 => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]
                ),
                AbstractStrategyFactory::ABS => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]
                ),
                AbstractStrategyFactory::WORKSPACE_REDSHIFT => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]
                ),
                AbstractStrategyFactory::WORKSPACE_SYNAPSE => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]
                ),
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]
                ),
                AbstractStrategyFactory::WORKSPACE_ABS => new Scope(
                    [Scope::TABLE_METADATA]
                ),
                AbstractStrategyFactory::WORKSPACE_EXASOL => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]
                ),
                AbstractStrategyFactory::WORKSPACE_TERADATA => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]
                ),
                AbstractStrategyFactory::WORKSPACE_BIGQUERY => new Scope(
                    [Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]
                ),
            ]
        );
    }
}
