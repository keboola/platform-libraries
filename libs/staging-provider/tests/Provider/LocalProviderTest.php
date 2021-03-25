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
use Keboola\WorkspaceProvider\Provider\LocalProvider;
use Keboola\WorkspaceProvider\Provider\RedshiftWorkspaceProvider;
use Keboola\WorkspaceProvider\Provider\SnowflakeWorkspaceProvider;
use PHPUnit\Framework\TestCase;

class LocalProviderTest extends TestCase
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
    }

    public function testPath()
    {
        $provider = new LocalProvider('somePath');
        $provider->cleanup(); // cleanup does nothing, doesn't throw error either
        self::assertEquals('somePath', $provider->getPath());
    }

    public function testWorkspace()
    {
        $provider = new LocalProvider('somePath');
        self::expectException(WorkspaceProviderException::class);
        self::expectExceptionMessage('Local provider has no workspace');
        $provider->getWorkspaceId();
    }

    public function testCredentials()
    {
        $provider = new LocalProvider('somePath');
        self::expectException(WorkspaceProviderException::class);
        self::expectExceptionMessage('Local provider has no workspace');
        $provider->getCredentials();
    }
}