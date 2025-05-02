<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Configuration\WorkspaceCredentials;
use Keboola\StagingProvider\Provider\Credentials\NoCredentialsProvider;
use Keboola\StagingProvider\Provider\ExistingWorkspaceProvider;
use Keboola\StagingProvider\Provider\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class NoCredentialsProviderTest extends TestCase
{
    public function testCredentialsThrowsExceptionByDefault(): void
    {
        $workspaceProvider = $this->createMock(ExistingWorkspaceProvider::class);
        $workspaceProvider->expects(self::never())->method(self::anything());

        $workspace = $this->createMock(Workspace::class);
        $workspace->expects(self::never())->method('setCredentialsFromData');

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Credentials are not available');

        $provider = new NoCredentialsProvider();
        $provider->provideCredentials($workspaceProvider, $workspace);
    }
}
