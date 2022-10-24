<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Staging\Workspace;

use Keboola\StagingProvider\Staging\Workspace\SnowflakeWorkspaceStaging;
use PHPUnit\Framework\TestCase;

class SnowflakeWorkspaceStagingTest extends TestCase
{
    /** @dataProvider provideAccountTestData */
    public function testGetCredentialsWithAccount(string $host, string $expectedAccount): void
    {
        $staging = new SnowflakeWorkspaceStaging([
            'connection' => [
                'backend' => 'snowflake',
                'host' => $host,
                'warehouse' => 'warehouse',
                'database' => 'database',
                'schema' => 'schema',
                'user' => 'user',
                'password' => 'password',
            ],
        ]);

        $credentials = $staging->getCredentials();
        self::assertSame($expectedAccount, $credentials['account']);
    }

    public function provideAccountTestData(): iterable
    {
        yield 'localhost' => [
            'host' => 'localhost',
            'account' => 'localhost',
        ];

        yield 'keboola.snowflakecomputing.com' => [
            'host' => 'keboola.snowflakecomputing.com',
            'account' => 'keboola',
        ];

        yield 'test.west-us-2.azure.snowflakecomputing.com' => [
            'host' => 'test.west-us-2.azure.snowflakecomputing.com',
            'account' => 'test',
        ];
    }
}
