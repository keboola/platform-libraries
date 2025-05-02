<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\StagingProvider\Provider\Configuration\WorkspaceCredentials;
use Keboola\StagingProvider\Provider\Credentials\ExistingCredentialsProvider;
use Keboola\StagingProvider\Provider\ExistingWorkspaceProvider;
use Keboola\StagingProvider\Provider\Workspace;
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

        $workspaceProvider = $this->createMock(ExistingWorkspaceProvider::class);
        $workspaceProvider->expects(self::never())->method(self::anything());

        $credentials = new WorkspaceCredentials([
            'password' => 'password-value',
        ]);

        $provider = new ExistingCredentialsProvider($credentials);
        $provider->provideCredentials($workspaceProvider, $workspace);

        self::assertTrue($workspace->hasCredentials());
        self::assertSame([
            'host' => 'some-host',
            'warehouse' => 'some-warehouse',
            'database' => 'some-database',
            'schema' => 'some-schema',
            'user' => 'some-user',
            'password' => 'password-value',
            'privateKey' => null,
            'account' => 'some-host',
        ], $workspace->getCredentials());
    }
}
