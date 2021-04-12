<?php

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\StagingProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\RedshiftWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SynapseWorkspaceStaging;

class InputProviderInitializer extends AbstractProviderInitializer
{
    public function initializeProviders(
        $stagingType,
        array $tokenInfo
    ) {
        if ($stagingType === InputStrategyFactory::WORKSPACE_REDSHIFT &&
            $tokenInfo['owner']['hasRedshift']
        ) {
            $this->addWorkspaceProvider(
                RedshiftWorkspaceStaging::class,
                [
                    InputStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === InputStrategyFactory::WORKSPACE_SNOWFLAKE &&
            $tokenInfo['owner']['hasSnowflake']
        ) {
            $this->addWorkspaceProvider(
                SnowflakeWorkspaceStaging::class,
                [
                    InputStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === InputStrategyFactory::WORKSPACE_SYNAPSE &&
            $tokenInfo['owner']['hasSynapse']
        ) {
            $this->addWorkspaceProvider(
                SynapseWorkspaceStaging::class,
                [
                    InputStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === InputStrategyFactory::WORKSPACE_ABS &&
            $tokenInfo['owner']['fileStorageProvider'] === 'azure'
        ) {
            $this->addWorkspaceProvider(
                AbsWorkspaceStaging::class,
                [
                    InputStrategyFactory::WORKSPACE_ABS => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA]),
                ]
            );
        }

        $this->addLocalProvider(
            [
                InputStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                // TABLE_DATA for ABS and S3 is bound to LocalProvider because it requires no provider at all
                InputStrategyFactory::S3 => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::ABS => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                InputStrategyFactory::WORKSPACE_ABS => new Scope([Scope::TABLE_METADATA]),
            ]
        );
    }
}
