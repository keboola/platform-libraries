<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider\Credentials;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Credentials\PrivateKeyWorkspaceCredentials;
use PHPUnit\Framework\TestCase;

class PrivateKeyWorkspaceCredentialsTest extends TestCase
{
    public function testFromPasswordResetArray(): void
    {
        $credentials = PrivateKeyWorkspaceCredentials::fromPasswordResetArray([
            'privateKey' => 'test-private-key',
        ]);

        self::assertSame(
            [
                'privateKey' => 'test-private-key',
            ],
            $credentials->toArray(),
        );
    }
}
