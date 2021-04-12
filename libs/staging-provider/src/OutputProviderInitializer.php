<?php

namespace Keboola\StagingProvider;

use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Staging\Workspace\AbsWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\RedshiftWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\SynapseWorkspaceStaging;
use Keboola\StagingProvider\WorkspaceProviderFactory\WorkspaceProviderFactoryInterface;

class OutputProviderInitializer extends AbstractProviderInitializer
{
    public function __construct(
        InputStrategyFactory $stagingFactory,
        WorkspaceProviderFactoryInterface $workspaceProviderFactory,
        $dataDirectory
    ) {
        if (!$stagingFactory instanceof OutputStrategyFactory) {
            throw new StagingProviderException(sprintf(
                'Given staging factory %s is not instance of %s',
                get_class($stagingFactory),
                OutputStrategyFactory::class
            ));
        }

        parent::__construct(
            $stagingFactory,
            $workspaceProviderFactory,
            $dataDirectory
        );
    }

    public function initializeProviders(
        $stagingType,
        array $tokenInfo
    ) {
        if ($stagingType === OutputStrategyFactory::WORKSPACE_REDSHIFT &&
            $tokenInfo['owner']['hasRedshift']
        ) {
            $this->addWorkspaceProvider(
                RedshiftWorkspaceStaging::class,
                [
                    OutputStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === OutputStrategyFactory::WORKSPACE_SNOWFLAKE &&
            $tokenInfo['owner']['hasSnowflake']
        ) {
            $this->addWorkspaceProvider(
                SnowflakeWorkspaceStaging::class,
                [
                    OutputStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === OutputStrategyFactory::WORKSPACE_SYNAPSE &&
            $tokenInfo['owner']['hasSynapse']
        ) {
            $this->addWorkspaceProvider(
                SynapseWorkspaceStaging::class,
                [
                    OutputStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::TABLE_DATA]),
                ]
            );
        }

        if ($stagingType === OutputStrategyFactory::WORKSPACE_ABS &&
            $tokenInfo['owner']['fileStorageProvider'] === 'azure'
        ) {
            $this->addWorkspaceProvider(
                AbsWorkspaceStaging::class,
                [
                    OutputStrategyFactory::WORKSPACE_ABS => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA]),
                ]
            );
        }
        
        $this->addLocalProvider(
            [
                OutputStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                OutputStrategyFactory::WORKSPACE_REDSHIFT => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                OutputStrategyFactory::WORKSPACE_SYNAPSE => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                OutputStrategyFactory::WORKSPACE_SNOWFLAKE => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA, Scope::TABLE_METADATA]),
                OutputStrategyFactory::WORKSPACE_ABS => new Scope([Scope::TABLE_METADATA]),
            ]
        );
    }
}
