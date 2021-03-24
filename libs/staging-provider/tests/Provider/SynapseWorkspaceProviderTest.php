<?php

namespace Keboola\WorkspaceProvider\Tests\Provider;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\WorkspaceProvider\Provider\SynapseWorkspaceProvider;
use PHPUnit\Framework\TestCase;

class SynapseWorkspaceProviderTest extends TestCase
{
    /**
     * @var Client
     */
    private $client;

    public function setUp()
    {
        parent::setUp();
        if (!getenv('RUN_SYNAPSE_TESTS')) {
            return;
        }
        $this->client = new Client([
            'url' => getenv('STORAGE_API_URL_SYNAPSE'),
            'token' => getenv('STORAGE_API_TOKEN_SYNAPSE'),
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

    public function testSynapseWorkspaceProvider()
    {
        if (!getenv('RUN_SYNAPSE_TESTS')) {
            self::markTestSkipped('Synapse test is disabled.');
        }
        $components = new Components($this->client);
        $configuration = new Configuration();
        $configuration->setComponentId('keboola.runner-workspace-test');
        $configuration->setName('runner-tests');
        $configuration->setConfigurationId('runner-test-configuration');
        $components->addConfiguration($configuration);
        $provider = new SynapseWorkspaceProvider($this->client, 'keboola.runner-workspace-test', 'runner-test-configuration');
        $workspaceId = $provider->getWorkspaceId();
        $workspaces = new Workspaces($this->client);
        $workspace = $workspaces->getWorkspace($workspaceId);
        self::assertEquals('keboola.runner-workspace-test', $workspace['component']);
        self::assertEquals('runner-test-configuration', $workspace['configurationId']);
        self::assertArrayHasKey('host', $workspace['connection']);
        self::assertArrayHasKey('database', $workspace['connection']);
        self::assertArrayHasKey('user', $workspace['connection']);
        self::assertEquals('synapse', $workspace['connection']['backend']);
        self::assertEquals(['host', 'warehouse', 'database', 'schema', 'user', 'password'], array_keys($provider->getCredentials()));
    }
}
