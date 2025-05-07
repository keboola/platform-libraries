<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StagingProvider\Provider\Credentials\ResetCredentialsProvider;
use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Provider\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class ResetCredentialsProviderTest extends TestCase
{
    public function testResetCredentialsWithPassword(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
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

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('resetCredentials')
            ->with((int) $workspaceId, new Workspaces\ResetCredentialsRequest())
            ->willReturn(['password' => 'new-password']);

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator->expects(self::never())->method(self::anything());

        $provider = new ResetCredentialsProvider($workspacesApiClient, $snowflakeKeypairGenerator);

        $result = $provider->provideCredentials($workspace);

        self::assertSame(['password' => 'new-password'], $result);
    }

    public function testResetCredentialsWithKeyPair(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
            ],
        ];

        $workspace = Workspace::createFromData($workspaceData);

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('resetCredentials')
            ->with((int) $workspaceId, new Workspaces\ResetCredentialsRequest(publicKey: 'public-key'))
        ;

        $snowflakeKeypairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $snowflakeKeypairGenerator
            ->expects(self::once())
            ->method('generateKeyPair')
            ->willReturn(new PemKeyCertificatePair('private-key', 'public-key'));

        $provider = new ResetCredentialsProvider($workspacesApiClient, $snowflakeKeypairGenerator);

        $result = $provider->provideCredentials($workspace);

        self::assertSame(['privateKey' => 'private-key'], $result);
    }
}
