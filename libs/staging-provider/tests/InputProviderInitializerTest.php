<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Strategy\ABSWorkspace as InputFileABSWorkspace;
use Keboola\InputMapping\File\Strategy\Local as InputFileLocal;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS as InputAbs;
use Keboola\InputMapping\Table\Strategy\ABSWorkspace as InputTableABSWorkspace;
use Keboola\InputMapping\Table\Strategy\BigQuery as InputTableBigQuery;
use Keboola\InputMapping\Table\Strategy\Exasol as InputTableExasol;
use Keboola\InputMapping\Table\Strategy\Local as InputTableLocal;
use Keboola\InputMapping\Table\Strategy\Redshift as InputTableRedshift;
use Keboola\InputMapping\Table\Strategy\S3 as InputS3;
use Keboola\InputMapping\Table\Strategy\Snowflake as InputTableSnowflake;
use Keboola\InputMapping\Table\Strategy\Synapse as InputTableSynapse;
use Keboola\InputMapping\Table\Strategy\Teradata as InputTableTeradata;
use Keboola\StagingProvider\InputProviderInitializer;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StagingProvider\Provider\NewWorkspaceStagingProvider;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class InputProviderInitializerTest extends TestCase
{
    public function testInitializeInputLocal(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::LOCAL, null, null, 'system'),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(AbstractStrategyFactory::LOCAL, []);

        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::LOCAL, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableLocal::class,
            $stagingFactory->getTableInputStrategy(AbstractStrategyFactory::LOCAL, '', new InputTableStateList([])),
        );
        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::ABS, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputAbs::class,
            $stagingFactory->getTableInputStrategy(AbstractStrategyFactory::ABS, '', new InputTableStateList([])),
        );
        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::S3, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputS3::class,
            $stagingFactory->getTableInputStrategy(AbstractStrategyFactory::S3, '', new InputTableStateList([])),
        );

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            '',
            new InputTableStateList([]),
        );
    }

    public function testInitializeInputRedshift(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_REDSHIFT, null, null, 'system'),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'aws',
                ],
            ],
        );

        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::LOCAL, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableLocal::class,
            $stagingFactory->getTableInputStrategy(AbstractStrategyFactory::LOCAL, '', new InputTableStateList([])),
        );
        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(
                AbstractStrategyFactory::WORKSPACE_REDSHIFT,
                new InputFileStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputTableRedshift::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::WORKSPACE_REDSHIFT,
                '',
                new InputTableStateList([]),
            ),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            '',
            new InputTableStateList([]),
        );
    }

    public function testInitializeInputSnowflake(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );
        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, null, null, 'system'),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'aws',
                ],
            ],
        );

        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::LOCAL, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableLocal::class,
            $stagingFactory->getTableInputStrategy(AbstractStrategyFactory::LOCAL, '', new InputTableStateList([])),
        );
        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
                new InputFileStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputTableSnowflake::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
                '',
                new InputTableStateList([]),
            ),
        );

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            '',
            new InputTableStateList([]),
        );
    }

    public function testInitializeInputSynapse(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_SYNAPSE, null, null, 'system'),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_SYNAPSE,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
        );

        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::LOCAL, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableLocal::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::LOCAL,
                '',
                new InputTableStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(
                AbstractStrategyFactory::WORKSPACE_SYNAPSE,
                new InputFileStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputTableSynapse::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::WORKSPACE_SYNAPSE,
                '',
                new InputTableStateList([]),
            ),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            '',
            new InputTableStateList([]),
        );
    }

    public function testInitializeInputAbs(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_ABS, null, null, 'system'),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_ABS,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
        );

        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::LOCAL, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableLocal::class,
            $stagingFactory->getTableInputStrategy(AbstractStrategyFactory::LOCAL, '', new InputTableStateList([])),
        );
        self::assertInstanceOf(
            InputFileABSWorkspace::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::WORKSPACE_ABS, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableABSWorkspace::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::WORKSPACE_ABS,
                '',
                new InputTableStateList([]),
            ),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            '',
            new InputTableStateList([]),
        );
    }

    public function testInitializeInputExasol(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_EXASOL, null, null, 'system'),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_EXASOL,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'hasExasol' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
        );

        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::LOCAL, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableLocal::class,
            $stagingFactory->getTableInputStrategy(AbstractStrategyFactory::LOCAL, '', new InputTableStateList([])),
        );
        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(
                AbstractStrategyFactory::WORKSPACE_EXASOL,
                new InputFileStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputTableExasol::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::WORKSPACE_EXASOL,
                '',
                new InputTableStateList([]),
            ),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            '',
            new InputTableStateList([]),
        );
    }

    public function testInitializeInputTeradata(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_TERADATA, null, null, 'system'),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_TERADATA,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'hasExasol' => true,
                    'hasTeradata' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
        );

        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::LOCAL, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableLocal::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::LOCAL,
                '',
                new InputTableStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(
                AbstractStrategyFactory::WORKSPACE_TERADATA,
                new InputFileStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputTableTeradata::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::WORKSPACE_TERADATA,
                '',
                new InputTableStateList([]),
            ),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            '',
            new InputTableStateList([]),
        );
    }

    public function testInitializeInputBigQuery(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new InputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_BIGQUERY, null, null, 'system'),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_BIGQUERY,
            [
                'owner' => [
                    'hasBigquery' => true,
                    'fileStorageProvider' => 'aws',
                ],
            ],
        );

        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(AbstractStrategyFactory::LOCAL, new InputFileStateList([])),
        );
        self::assertInstanceOf(
            InputTableLocal::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::LOCAL,
                '',
                new InputTableStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputFileLocal::class,
            $stagingFactory->getFileInputStrategy(
                AbstractStrategyFactory::WORKSPACE_BIGQUERY,
                new InputFileStateList([]),
            ),
        );
        self::assertInstanceOf(
            InputTableBigQuery::class,
            $stagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::WORKSPACE_BIGQUERY,
                '',
                new InputTableStateList([]),
            ),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            '',
            new InputTableStateList([]),
        );
    }
}
