<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\WorkspaceProviderFactory\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\WorkspaceProviderFactory\Credentials\ABSWorkspaceCredentials;
use PHPUnit\Framework\TestCase;

class ABSWorkspaceCredentialsTest extends TestCase
{
    public function testCreateSuccess(): void
    {
        $response = [
            'connectionString' => 'BlobEndpoint=https://kbcfshcwhatever',
        ];
        $credentials = ABSWorkspaceCredentials::fromPasswordResetArray($response);
        self::assertSame($response, $credentials->toArray());
    }

    public function testCreateFailure(): void
    {
        $response = [
            'password' => 'BlobEndpoint=https://kbcfshcwhatever',
        ];
        // phpcs:ignore Generic.Files.LineLength.MaxExceeded
        $this->expectExceptionMessage('Invalid password reset response for ABS backend "Undefined array key "connectionString"" - keys: ["password"]');
        $this->expectException(StagingProviderException::class);
        ABSWorkspaceCredentials::fromPasswordResetArray($response);
    }
}
