<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\Writer\File\Strategy\Local as OutputFileLocal;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;
use Keboola\StagingProvider\OutputProviderInitializer;
use Keboola\StagingProvider\Provider\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StagingProvider\Provider\NewWorkspaceProvider;
use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\WorkspaceLoginType;
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

        $workspaceStagingProvider = new NewWorkspaceProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
            new WorkspaceBackendConfig(
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
                null,
                null,
                NetworkPolicy::SYSTEM,
                null,
            ),
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

        $this->expectExceptionMessage('The project does not support "workspace-bigquery" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_BIGQUERY);
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

        $workspaceStagingProvider = new NewWorkspaceProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
            new WorkspaceBackendConfig(
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
                null,
                null,
                NetworkPolicy::SYSTEM,
                WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD,
            ),
            'my-test-component',
            'my-test-config',
        );
        $localStagingProvider = new LocalStagingProvider('/tmp/random/data');
        $init = new OutputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            [
                'owner' => [
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

        $this->expectExceptionMessage('The project does not support "workspace-bigquery" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_BIGQUERY);
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

        $workspaceStagingProvider = new NewWorkspaceProvider(
            new Workspaces($clientWrapper->getBasicClient()),
            new Components($clientWrapper->getBasicClient()),
            new SnowflakeKeypairGenerator(new PemKeyCertificateGenerator()),
            new WorkspaceBackendConfig(
                AbstractStrategyFactory::WORKSPACE_BIGQUERY,
                null,
                null,
                NetworkPolicy::SYSTEM,
                null,
            ),
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
