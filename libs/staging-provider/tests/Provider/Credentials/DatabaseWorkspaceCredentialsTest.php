<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Credentials\DatabaseWorkspaceCredentials;
use PHPUnit\Framework\TestCase;

class DatabaseWorkspaceCredentialsTest extends TestCase
{
    public function testCreateSuccess(): void
    {
        $response = [
            'password' => 'bmxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        ];
        $credentials = DatabaseWorkspaceCredentials::fromPasswordResetArray($response);
        self::assertSame($response, $credentials->toArray());
    }

    public function testCreateFailure(): void
    {
        $response = [
            'connectionString' => 'BlobEndpoint=https://kbcfshcwhatever',
        ];
        // phpcs:ignore Generic.Files.LineLength.MaxExceeded
        $this->expectExceptionMessage('Invalid password reset response for DB backend "Undefined array key "password"" - keys: ["connectionString"]');
        $this->expectException(StagingProviderException::class);
        DatabaseWorkspaceCredentials::fromPasswordResetArray($response);
    }
}
