<?php

namespace Keboola\StagingProvider\Tests;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StagingProvider\InputProviderInitializer;
use Keboola\StagingProvider\OutputProviderInitializer;
use Keboola\StagingProvider\WorkspaceProviderFactory\ComponentWorkspaceProviderFactory;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionProperty;

class CombinedProviderInitializerTest extends TestCase
{
    /** @var Client */
    private $client;

    protected function setUp()
    {
        parent::setUp();

        $this->client = new Client([
            'url' => getenv('STORAGE_API_URL'),
            'token' => getenv('STORAGE_API_TOKEN'),
        ]);
    }


    public function testWorkspaceIsInitializedOnlyOnce()
    {
        $clientWrapper = new ClientWrapper(
            $this->client,
            null,
            new NullLogger(),
            ''
        );
        $logger = new NullLogger();

        $componentsApi = new Components($this->client);
        $configuration = new Configuration();
        $componentId = 'keboola.runner-workspace-test';
        $configuration->setComponentId($componentId);
        $configuration->setName('test-config');
        $configId = uniqid('my-test-config');
        $configuration->setConfigurationId($configId);
        $configuration->setConfiguration([]);
        $componentsApi->addConfiguration($configuration);
        $workspacesApi = new Workspaces($this->client);

        try {
            $providerFactory = new ComponentWorkspaceProviderFactory(
                $componentsApi,
                $workspacesApi,
                $componentId,
                $configId
            );

            $inputStagingFactory = new InputStrategyFactory($clientWrapper, $logger, 'json');
            $inputInitializer = new InputProviderInitializer($inputStagingFactory, $providerFactory);
            $inputInitializer->initializeProviders(
                InputStrategyFactory::WORKSPACE_SNOWFLAKE,
                [
                    'owner' => ['hasSnowflake' => true],
                ],
                '/tmp/random/data'
            );

            $outputStagingFactory = new OutputStrategyFactory($clientWrapper, $logger, 'json');
            $outputInitializer = new OutputProviderInitializer($outputStagingFactory, $providerFactory);
            $outputInitializer->initializeProviders(
                OutputStrategyFactory::WORKSPACE_SNOWFLAKE,
                [
                    'owner' => ['hasSnowflake' => true],
                ],
                '/tmp/random/data'
            );

            $inputStrategy = $inputStagingFactory->getTableInputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE, 'test', new InputTableStateList([]));
            $reflection = new ReflectionProperty($inputStrategy, 'dataStorage');
            $reflection->setAccessible(true);
            /** @var ProviderInterface $dataStorage */
            $dataStorage = $reflection->getValue($inputStrategy);
            $workspaceId1 = $dataStorage->getWorkspaceId();

            $outputStrategy = $outputStagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE);
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
