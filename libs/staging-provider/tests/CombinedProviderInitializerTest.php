<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StagingProvider\InputProviderInitializer;
use Keboola\StagingProvider\OutputProviderInitializer;
use Keboola\StagingProvider\Provider\Configuration\NetworkPolicy;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceBackendConfig;
use Keboola\StagingProvider\Provider\LocalStagingProvider;
use Keboola\StagingProvider\Provider\NewWorkspaceStagingProvider;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionProperty;

class CombinedProviderInitializerTest extends TestCase
{
    public function testWorkspaceIsInitializedOnlyOnce(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
            ),
        );
        $logger = new NullLogger();

        $componentsApi = new Components($clientWrapper->getBasicClient());
        $configuration = new Configuration();
        $componentId = 'keboola.runner-workspace-test';
        $configuration->setComponentId($componentId);
        $configuration->setName('test-config');
        $configId = uniqid('my-test-config');
        $configuration->setConfigurationId($configId);
        $configuration->setConfiguration([]);
        $componentsApi->addConfiguration($configuration);
        $workspacesApi = new Workspaces($clientWrapper->getBasicClient());

        try {
            $workspaceStagingProvider = new NewWorkspaceStagingProvider(
                $workspacesApi,
                $componentsApi,
                new WorkspaceBackendConfig(
                    AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
                    null,
                    null,
                    NetworkPolicy::SYSTEM,
                    WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD,
                ),
                $componentId,
                $configId,
            );
            $localStagingProvider = new LocalStagingProvider('/tmp/random/data');

            $inputStagingFactory = new InputStrategyFactory($clientWrapper, $logger, 'json');
            $inputInitializer = new InputProviderInitializer(
                $inputStagingFactory,
                $workspaceStagingProvider,
                $localStagingProvider,
            );
            $inputInitializer->initializeProviders(
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
                [
                    'owner' => ['hasSnowflake' => true],
                ],
            );

            $outputStagingFactory = new OutputStrategyFactory($clientWrapper, $logger, 'json');
            $outputInitializer = new OutputProviderInitializer(
                $outputStagingFactory,
                $workspaceStagingProvider,
                $localStagingProvider,
            );
            $outputInitializer->initializeProviders(
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
                [
                    'owner' => ['hasSnowflake' => true],
                ],
            );

            $inputStrategy = $inputStagingFactory->getTableInputStrategy(
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
                'test',
                new InputTableStateList([]),
            );
            $reflection = new ReflectionProperty($inputStrategy, 'dataStorage');
            $reflection->setAccessible(true);
            /** @var ProviderInterface $dataStorage */
            $dataStorage = $reflection->getValue($inputStrategy);
            $workspaceId1 = $dataStorage->getWorkspaceId();

            $outputStrategy = $outputStagingFactory->getTableOutputStrategy(
                AbstractStrategyFactory::WORKSPACE_SNOWFLAKE,
            );
            $reflection = new ReflectionProperty($outputStrategy, 'dataStorage');
            $reflection->setAccessible(true);
            /** @var ProviderInterface $dataStorage */
            $dataStorage = $reflection->getValue($outputStrategy);
            $workspaceId2 = $dataStorage->getWorkspaceId();

            self::assertEquals($workspaceId1, $workspaceId2);
        } finally {
            $componentsApi->deleteConfiguration($componentId, $configId);
        }
    }
}
