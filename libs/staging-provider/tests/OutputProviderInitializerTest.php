<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\Writer\File\Strategy\ABSWorkspace;
use Keboola\OutputMapping\Writer\File\Strategy\Local as OutputFileLocal;
use Keboola\OutputMapping\Writer\Table\Strategy\AbsWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\StagingProvider\OutputProviderInitializer;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StagingProvider\Provider\NewWorkspaceStagingProvider;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class OutputProviderInitializerTest extends TestCase
{
    public function testInitializeOutputLocal(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::LOCAL, null, null),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::LOCAL,
            [],
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_REDSHIFT);
    }

    public function testInitializeOutputRedshift(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_REDSHIFT, null, null),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
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
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_REDSHIFT),
        );
        self::assertInstanceOf(
            SqlWorkspaceTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_REDSHIFT),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputSnowflake(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, null, null),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
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
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE),
        );
        self::assertInstanceOf(
            SqlWorkspaceTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE),
        );

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_REDSHIFT);
    }

    public function testInitializeOutputSynapse(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_SYNAPSE, null, null),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

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
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_SYNAPSE),
        );
        self::assertInstanceOf(
            SqlWorkspaceTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SYNAPSE),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputAbs(): void
    {
        if (!getenv('RUN_SYNAPSE_TESTS')) {
            self::markTestSkipped('Synapse test is disabled.');
        }

        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('SYNAPSE_STORAGE_API_URL'),
                (string) getenv('SYNAPSE_STORAGE_API_TOKEN'),
            ),
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $components = new Components($stagingFactory->getClientWrapper()->getBasicClient());
        try {
            $components->deleteConfiguration('keboola.runner-workspace-abs-test', 'my-test-config');
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        $configuration = new Configuration();
        $configuration->setConfigurationId('my-test-config');
        $configuration->setName($configuration->getConfigurationId());
        $configuration->setComponentId('keboola.runner-workspace-abs-test');
        $components->addConfiguration($configuration);

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_ABS, null, null),
            'keboola.runner-workspace-abs-test',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

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
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            ABSWorkspace::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS),
        );
        self::assertInstanceOf(
            AbsWorkspaceTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputExasol(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_EXASOL, null, null),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_EXASOL,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasExasol' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_EXASOL),
        );
        self::assertInstanceOf(
            SqlWorkspaceTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_EXASOL),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputTeradata(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_TERADATA, null, null),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_TERADATA,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasExasol' => true,
                    'hasSnowflake' => true,
                    'hasTeradata' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_TERADATA),
        );
        self::assertInstanceOf(
            SqlWorkspaceTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_TERADATA),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputBigQuery(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json',
        );

        $workspaceStagingProvider = new NewWorkspaceStagingProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new WorkspaceBackendConfig(AbstractStrategyFactory::WORKSPACE_BIGQUERY, null, null),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

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
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
        self::assertInstanceOf(
            OutputFileLocal::class,
            $stagingFactory->getFileOutputStrategy(AbstractStrategyFactory::WORKSPACE_BIGQUERY),
        );
        self::assertInstanceOf(
            SqlWorkspaceTableStrategy::class,
            $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_BIGQUERY),
        );

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);
    }
}
