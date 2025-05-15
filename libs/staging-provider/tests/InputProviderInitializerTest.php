<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Strategy\Local as InputFileLocal;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS as InputAbs;
use Keboola\InputMapping\Table\Strategy\BigQuery as InputTableBigQuery;
use Keboola\InputMapping\Table\Strategy\Local as InputTableLocal;
use Keboola\InputMapping\Table\Strategy\S3 as InputS3;
use Keboola\InputMapping\Table\Strategy\Snowflake as InputTableSnowflake;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\StagingProvider\InputProviderInitializer;
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

        $this->expectExceptionMessage('The project does not support "workspace-bigquery" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_BIGQUERY,
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
        $init = new InputProviderInitializer($stagingFactory, $workspaceStagingProvider, $localStagingProvider);

        $init->initializeProviders(
            AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            [
                'owner' => [
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

        $this->expectExceptionMessage('The project does not support "workspace-bigquery" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(
            AbstractStrategyFactory::WORKSPACE_BIGQUERY,
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
