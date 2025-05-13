<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Workspace\Configuration\WorkspaceCredentials;
use Keboola\StagingProvider\Workspace\Credentials\NoCredentialsProvider;
use Keboola\StagingProvider\Workspace\ExistingWorkspaceProvider;
use Keboola\StagingProvider\Workspace\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class NoCredentialsProviderTest extends TestCase
{
    public function testCredentialsThrowsExceptionByDefault(): void
    {
        $workspaceData = [
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'loginType' => WorkspaceLoginType::DEFAULT->value,
            ],
        ];

        $workspace = Workspace::createFromData($workspaceData);
        $provider = new NoCredentialsProvider();

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Credentials are not available');
        $provider->provideCredentials($workspace);
    }
}
