<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Credentials\ResetCredentialsProvider;
use Keboola\StagingProvider\Provider\ExistingWorkspaceProvider;
use Keboola\StagingProvider\Provider\SnowflakeKeypairGenerator;
use Keboola\StagingProvider\Provider\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class ResetCredentialsProviderTest extends TestCase
{
    public function testResetCredentialsWithPassword(): void
    {
        $workspace = $this->createMock(Workspace::class);
        $workspace->method('getLoginType')->willReturn(WorkspaceLoginType::DEFAULT);
        $workspace->expects(self::never())->method('setCredentialsFromData');

        $keyPairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $keyPairGenerator->expects(self::never())->method(self::anything());

        $workspaceProvider = $this->createMock(ExistingWorkspaceProvider::class);
        $workspaceProvider->expects(self::once())
            ->method('resetCredentials')
            ->with([]);

        $provider = new ResetCredentialsProvider($keyPairGenerator);
        $provider->provideCredentials($workspaceProvider, $workspace);
    }

    public function testResetCredentialsWithKeyPair(): void
    {
        $keyPair = new PemKeyCertificatePair('private-key', 'public-key');

        $keyPairGenerator = $this->createMock(SnowflakeKeypairGenerator::class);
        $keyPairGenerator->expects(self::once())
            ->method('generateKeyPair')
            ->willReturn($keyPair);

        $workspace = $this->createMock(Workspace::class);
        $workspace->method('getLoginType')->willReturn(WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR);
        $workspace->expects(self::once())
            ->method('setCredentialsFromData')
            ->with([
                'privateKey' => 'private-key',
            ]);

        $workspaceProvider = $this->createMock(ExistingWorkspaceProvider::class);
        $workspaceProvider->expects(self::once())
            ->method('resetCredentials')
            ->with([
                'publicKey' => 'public-key',
            ]);

        $provider = new ResetCredentialsProvider($keyPairGenerator);
        $provider->provideCredentials($workspaceProvider, $workspace);
    }
}
