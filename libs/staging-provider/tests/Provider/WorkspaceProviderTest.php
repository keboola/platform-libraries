<?php

namespace Keboola\WorkspaceProvider\Tests\Provider;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\WorkspaceProvider\Exception\WorkspaceProviderException;
use Keboola\WorkspaceProvider\Provider\AbstractWorkspaceProvider;
use Keboola\WorkspaceProvider\Provider\RedshiftWorkspaceProvider;
use Keboola\WorkspaceProvider\Provider\SnowflakeWorkspaceProvider;
use PHPUnit\Framework\TestCase;

class WorkspaceProviderTest extends TestCase
{
    /**
     * @var Client
     */
    private $client;

    public function setUp()
    {
        parent::setUp();
        $this->client = new Client([
            'url' => getenv('STORAGE_API_URL'),
            'token' => getenv('STORAGE_API_TOKEN'),
        ]);
        $components = new Components($this->client);
        $workspaces = new Workspaces($this->client);
        $options = new ListComponentConfigurationsOptions();
        $options->setComponentId('keboola.runner-workspace-test');
        foreach ($components->listComponentConfigurations($options) as $configuration) {
            $wOptions = new ListConfigurationWorkspacesOptions();
            $wOptions->setComponentId('keboola.runner-workspace-test');
            $wOptions->setConfigurationId($configuration['id']);
            foreach ($components->listConfigurationWorkspaces($wOptions) as $workspace) {
                $workspaces->deleteWorkspace($workspace['id']);
            }
            $components->deleteConfiguration('keboola.runner-workspace-test', $configuration['id']);
        }
    }

    /**
     * @dataProvider workspaceTypeProvider
     * @param string $className
     * @param string $backendType
     */
    public function testWorkspaceProvider($className, $backendType)
    {
        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration->setComponentId('keboola.runner-workspace-test');
        $configuration->setName('runner-tests');
        $configuration->setConfigurationId('runner-test-configuration');
        $components->addConfiguration($configuration);
        $provider = new $className($this->client, 'keboola.runner-workspace-test', 'runner-test-configuration');
        /** @var AbstractWorkspaceProvider $provider */
        $workspaceId = $provider->getWorkspaceId();
        $workspaces = new Workspaces($this->client);
        $workspace = $workspaces->getWorkspace($workspaceId);
        self::assertEquals('keboola.runner-workspace-test', $workspace['component']);
        self::assertEquals('runner-test-configuration', $workspace['configurationId']);
        self::assertArrayHasKey('host', $workspace['connection']);
        self::assertArrayHasKey('database', $workspace['connection']);
        self::assertArrayHasKey('user', $workspace['connection']);
        self::assertEquals($backendType, $workspace['connection']['backend']);
        self::assertEquals(['host', 'warehouse', 'database', 'schema', 'user', 'password'], array_keys($provider->getCredentials()));
    }

    public function workspaceTypeProvider()
    {
        return [
            'redshift' => [RedshiftWorkspaceProvider::class, 'redshift'],
            'snowflake' => [SnowflakeWorkspaceProvider::class, 'snowflake'],
        ];
    }

    public function testEmptyConfiguration()
    {
        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration->setComponentId('keboola.runner-workspace-test');
        $configuration->setName('runner-tests');
        $configuration->setConfigurationId('runner-test-configuration');
        $components->addConfiguration($configuration);
        $provider = new SnowflakeWorkspaceProvider($this->client, 'keboola.runner-workspace-test', null);
        $workspaceId = $provider->getWorkspaceId();
        $workspaces = new Workspaces($this->client);
        $workspace = $workspaces->getWorkspace($workspaceId);
        self::assertEquals(null, $workspace['component']);
        self::assertEquals(null, $workspace['configurationId']);
        self::assertArrayHasKey('host', $workspace['connection']);
        self::assertArrayHasKey('database', $workspace['connection']);
        self::assertArrayHasKey('user', $workspace['connection']);
        self::assertEquals('snowflake', $workspace['connection']['backend']);
    }

    public function testLazyLoad()
    {
        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration->setComponentId('keboola.runner-workspace-test');
        $configuration->setName('runner-tests');
        $configuration->setConfigurationId('runner-test-configuration');
        $components->addConfiguration($configuration);
        $provider = new SnowflakeWorkspaceProvider($this->client, 'keboola.runner-workspace-test', 'runner-test-configuration');
        $options = new ListConfigurationWorkspacesOptions();
        $options->setComponentId('keboola.runner-workspace-test');
        $options->setConfigurationId('runner-test-configuration');
        self::assertCount(0, $components->listConfigurationWorkspaces($options));
        $provider->getWorkspaceId();
        self::assertCount(1, $components->listConfigurationWorkspaces($options));
    }

    public function testPath()
    {
        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration->setComponentId('keboola.runner-workspace-test');
        $configuration->setName('runner-tests');
        $configuration->setConfigurationId('runner-test-configuration');
        $components->addConfiguration($configuration);
        $provider = new SnowflakeWorkspaceProvider($this->client, 'keboola.runner-workspace-test', 'runner-test-configuration');
        self::expectException(WorkspaceProviderException::class);
        self::expectExceptionMessage('Workspace provides no path.');
        $provider->getPath();
    }

    public function testCleanup()
    {
        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration->setComponentId('keboola.runner-workspace-test');
        $configuration->setName('runner-tests');
        $configuration->setConfigurationId('runner-test-configuration');
        $components->addConfiguration($configuration);
        $provider = new SnowflakeWorkspaceProvider($this->client, 'keboola.runner-workspace-test', 'runner-test-configuration');
        $options = new ListConfigurationWorkspacesOptions();
        $options->setComponentId('keboola.runner-workspace-test');
        $options->setConfigurationId('runner-test-configuration');
        $provider->getWorkspaceId();
        self::assertCount(1, $components->listConfigurationWorkspaces($options));
        $provider->cleanup();
        self::assertCount(0, $components->listConfigurationWorkspaces($options));
    }
}
