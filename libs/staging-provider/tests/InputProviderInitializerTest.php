<?php

namespace Keboola\StagingProvider\Tests;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Strategy\ABSWorkspace as InputFileABSWorkspace;
use Keboola\InputMapping\File\Strategy\Local as InputFileLocal;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS as InputAbs;
use Keboola\InputMapping\Table\Strategy\ABSWorkspace as InputTableABSWorkspace;
use Keboola\InputMapping\Table\Strategy\Local as InputTableLocal;
use Keboola\InputMapping\Table\Strategy\Redshift as InputTableRedshift;
use Keboola\InputMapping\Table\Strategy\S3 as InputS3;
use Keboola\InputMapping\Table\Strategy\BigQuery as InputTableBigQuery;
use Keboola\InputMapping\Table\Strategy\Snowflake as InputTableSnowflake;
use Keboola\InputMapping\Table\Strategy\Synapse as InputTableSynapse;
use Keboola\InputMapping\Table\Strategy\Exasol as InputTableExasol;
use Keboola\InputMapping\Table\Strategy\Teradata as InputTableTeradata;
use Keboola\StagingProvider\WorkspaceProviderFactory\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StagingProvider\InputProviderInitializer;
use Keboola\StagingProvider\WorkspaceProviderFactory\ComponentWorkspaceProviderFactory;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class InputProviderInitializerTest extends TestCase
{
    public function testInitializeInputLocal()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
        );

        $providerFactory = new ComponentWorkspaceProviderFactory(
            new Components($clientWrapper->getBasicClient()),
            new Workspaces($clientWrapper->getBasicClient()),
            'my-test-component',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new InputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(InputStrategyFactory::LOCAL, []);

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::ABS, new InputFileStateList([])));
        self::assertInstanceOf(InputAbs::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::ABS, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::S3, new InputFileStateList([])));
        self::assertInstanceOf(InputS3::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::S3, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_REDSHIFT, '', new InputTableStateList([]));
    }

    public function testInitializeInputRedshift()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
        );

        $providerFactory = new ComponentWorkspaceProviderFactory(
            new Components($clientWrapper->getBasicClient()),
            new Workspaces($clientWrapper->getBasicClient()),
            'my-test-component',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new InputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            InputStrategyFactory::WORKSPACE_REDSHIFT,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'aws',
                ],
            ]
        );

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_REDSHIFT, new InputFileStateList([])));
        self::assertInstanceOf(InputTableRedshift::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_REDSHIFT, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }

    public function testInitializeInputSnowflake()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
        );
        $providerFactory = new ComponentWorkspaceProviderFactory(
            new Components($clientWrapper->getBasicClient()),
            new Workspaces($clientWrapper->getBasicClient()),
            'my-test-component',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new InputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            InputStrategyFactory::WORKSPACE_SNOWFLAKE,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'aws',
                ],
            ]
        );

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, new InputFileStateList([])));
        self::assertInstanceOf(InputTableSnowflake::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_REDSHIFT, '', new InputTableStateList([]));
    }

    public function testInitializeInputSynapse()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
        );

        $providerFactory = new ComponentWorkspaceProviderFactory(
            new Components($clientWrapper->getBasicClient()),
            new Workspaces($clientWrapper->getBasicClient()),
            'my-test-component',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new InputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            InputStrategyFactory::WORKSPACE_SYNAPSE,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_SYNAPSE, new InputFileStateList([])));
        self::assertInstanceOf(InputTableSynapse::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SYNAPSE, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }

    public function testInitializeInputAbs()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
        );

        $providerFactory = new ComponentWorkspaceProviderFactory(
            new Components($clientWrapper->getBasicClient()),
            new Workspaces($clientWrapper->getBasicClient()),
            'my-test-component',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new InputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            InputStrategyFactory::WORKSPACE_ABS,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileABSWorkspace::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_ABS, new InputFileStateList([])));
        self::assertInstanceOf(InputTableABSWorkspace::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_ABS, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }

    public function testInitializeInputExasol()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
        );

        $providerFactory = new ComponentWorkspaceProviderFactory(
            new Components($clientWrapper->getBasicClient()),
            new Workspaces($clientWrapper->getBasicClient()),
            'my-test-component',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new InputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            InputStrategyFactory::WORKSPACE_EXASOL,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'hasExasol' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_EXASOL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableExasol::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_EXASOL, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }

    public function testInitializeInputTeradata()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
        );

        $providerFactory = new ComponentWorkspaceProviderFactory(
            new Components($clientWrapper->getBasicClient()),
            new Workspaces($clientWrapper->getBasicClient()),
            'my-test-component',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new InputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            InputStrategyFactory::WORKSPACE_TERADATA,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'hasExasol' => true,
                    'hasTeradata' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_TERADATA, new InputFileStateList([])));
        self::assertInstanceOf(InputTableTeradata::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_TERADATA, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }

    public function testInitializeInputBigQuery()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
        );

        $providerFactory = new ComponentWorkspaceProviderFactory(
            new Components($clientWrapper->getBasicClient()),
            new Workspaces($clientWrapper->getBasicClient()),
            'my-test-component',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new InputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            InputStrategyFactory::WORKSPACE_BIGQUERY,
            [
                'owner' => [
                    'hasBigquery' => true,
                    'fileStorageProvider' => 'aws',
                ],
            ]
        );

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL, new InputFileStateList([])));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_BIGQUERY, new InputFileStateList([])));
        self::assertInstanceOf(InputTableBigQuery::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_BIGQUERY, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }
}
