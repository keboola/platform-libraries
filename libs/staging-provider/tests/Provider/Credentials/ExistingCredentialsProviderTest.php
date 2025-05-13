<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\StagingProvider\Workspace\Configuration\WorkspaceCredentials;
use Keboola\StagingProvider\Workspace\Credentials\CredentialsProvider;
use Keboola\StagingProvider\Workspace\ExistingWorkspaceProvider;
use Keboola\StagingProvider\Workspace\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class ExistingCredentialsProviderTest extends TestCase
{
    public function testWorkspaceGetters(): void
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

        $credentials = new WorkspaceCredentials([
            'password' => 'password-value',
        ]);

        $provider = new CredentialsProvider($credentials);
        $result = $provider->provideCredentials($workspace);

        self::assertSame([
            'password' => 'password-value',
        ], $result);
    }
}
