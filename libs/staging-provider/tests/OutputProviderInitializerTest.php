<?php

namespace Keboola\StagingProvider\Tests;

use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\Writer\File\Strategy\ABSWorkspace;
use Keboola\OutputMapping\Writer\File\Strategy\Local as OutputFileLocal;
use Keboola\OutputMapping\Writer\Table\Strategy\AbsWorkspaceTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\StagingProvider\WorkspaceProviderFactory\Configuration\WorkspaceBackendConfig;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StagingProvider\OutputProviderInitializer;
use Keboola\StagingProvider\WorkspaceProviderFactory\ComponentWorkspaceProviderFactory;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class OutputProviderInitializerTest extends TestCase
{
    public function testInitializeOutputLocal()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new OutputStrategyFactory(
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
        $init = new OutputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            OutputStrategyFactory::LOCAL,
            []
        );
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(LocalTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_REDSHIFT);
    }

    public function testInitializeOutputRedshift()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new OutputStrategyFactory(
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
        $init = new OutputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            OutputStrategyFactory::WORKSPACE_REDSHIFT,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(LocalTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::WORKSPACE_REDSHIFT));
        self::assertInstanceOf(SqlWorkspaceTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_REDSHIFT));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputSnowflake()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new OutputStrategyFactory(
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
        $init = new OutputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            OutputStrategyFactory::WORKSPACE_SNOWFLAKE,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(LocalTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE));
        self::assertInstanceOf(SqlWorkspaceTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE));

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_REDSHIFT);
    }

    public function testInitializeOutputSynapse()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new OutputStrategyFactory(
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
        $init = new OutputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            OutputStrategyFactory::WORKSPACE_SYNAPSE,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );

        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(LocalTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::WORKSPACE_SYNAPSE));
        self::assertInstanceOf(SqlWorkspaceTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SYNAPSE));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputAbs()
    {
        if (!getenv('RUN_SYNAPSE_TESTS')) {
            self::markTestSkipped('Synapse test is disabled.');
        }

        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL_SYNAPSE'),
                (string) getenv('STORAGE_API_TOKEN_SYNAPSE'),
            )
        );
        $stagingFactory = new OutputStrategyFactory(
            $clientWrapper,
            new NullLogger(),
            'json'
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

        $providerFactory = new ComponentWorkspaceProviderFactory(
            $components,
            new Workspaces($stagingFactory->getClientWrapper()->getBasicClient()),
            'keboola.runner-workspace-abs-test',
            'my-test-config',
            new WorkspaceBackendConfig(null)
        );
        $init = new OutputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            OutputStrategyFactory::WORKSPACE_ABS,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );

        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(LocalTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(ABSWorkspace::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::WORKSPACE_ABS));
        self::assertInstanceOf(AbsWorkspaceTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_ABS));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputExasol()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN'))
        );
        $stagingFactory = new OutputStrategyFactory(
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
        $init = new OutputProviderInitializer($stagingFactory, $providerFactory, '/tmp/random/data');

        $init->initializeProviders(
            OutputStrategyFactory::WORKSPACE_EXASOL,
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasExasol' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ]
        );
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(LocalTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::WORKSPACE_EXASOL));
        self::assertInstanceOf(SqlWorkspaceTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_EXASOL));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE);
    }
}
