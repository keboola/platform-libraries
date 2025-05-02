<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\WorkspaceCredentialsReset;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use PHPUnit\Framework\TestCase;

class WorkspaceCredentialsResetTest extends TestCase
{
    public function testResetPasswordCredentials(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
            'connection' => [
                'loginType' => WorkspaceLoginType::DEFAULT->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $workspacesApiClient
            ->expects(self::once())
            ->method('resetWorkspacePassword')
            ->with((int) $workspaceId)
            ->willReturn([
                'password' => 'new-password',
            ]);

        $reset = new WorkspaceCredentialsReset($workspacesApiClient);
        $result = $reset->resetWorkspaceCredentials($workspaceId, []);

        self::assertSame(['password' => 'new-password'], $result);
    }

    public function testResetKeyPairCredentials(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
            'connection' => [
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $workspacesApiClient
            ->expects(self::never())
            ->method('resetWorkspacePassword');

        $reset = new WorkspaceCredentialsReset($workspacesApiClient);
        $result = $reset->resetWorkspaceCredentials($workspaceId, ['publicKey' => 'public-key']);

        self::assertSame([], $result);
    }

    public function testResetKeyPairCredentialsWithInvalidParams(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
            'connection' => [
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $reset = new WorkspaceCredentialsReset($workspacesApiClient);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid parameters for key-pair authentication');
        $reset->resetWorkspaceCredentials($workspaceId, ['invalid' => 'param']);
    }

    public function testResetKeyPairCredentialsWithMissingPublicKey(): void
    {
        $workspaceId = '123456';
        $workspaceData = [
            'id' => $workspaceId,
            'connection' => [
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
            ],
        ];

        $workspacesApiClient = $this->createMock(Workspaces::class);
        $workspacesApiClient
            ->expects(self::once())
            ->method('getWorkspace')
            ->with((int) $workspaceId)
            ->willReturn($workspaceData);

        $reset = new WorkspaceCredentialsReset($workspacesApiClient);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid parameters for key-pair authentication');
        $reset->resetWorkspaceCredentials($workspaceId, []);
    }
}
