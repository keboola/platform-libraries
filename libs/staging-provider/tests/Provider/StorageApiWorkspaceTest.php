<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Generator;
use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\StorageApiWorkspace;
use PHPUnit\Framework\TestCase;

class StorageApiWorkspaceTest extends TestCase
{
    /** @dataProvider workspaceDataProvider */
    public function testFromDataArray(
        array $input,
        string $expectedId,
        string $expectedBackend,
        ?string $expectedBackendSize,
        array $expectedCredentials,
    ): void {
        $workspace = StorageApiWorkspace::fromDataArray($input);
        self::assertSame($expectedId, $workspace->id);
        self::assertSame($expectedBackend, $workspace->backend);
        self::assertSame($expectedBackendSize, $workspace->backendSize);
        self::assertSame($expectedCredentials, $workspace->credentials);
    }

    public function workspaceDataProvider(): Generator
    {
        yield 'abs' => [
            'input' => [
                'id' => '123456',
                'backendSize' => null,
                'connection' => [
                    'backend' => 'abs',
                    'container' => 'some-host',
                    'connectionString' => 'some-warehouse',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'abs',
            'expectedBackendSize' => null,
            'expectedCredentials' => [
                'container' => 'some-host',
                'connectionString' => 'some-warehouse',
            ],
        ];
        yield 'bigquery' => [
            'input' => [
                'id' => '123456',
                'backendSize' => null,
                'connection' => [
                    'backend' => 'bigquery',
                    'schema' => 'some-schema',
                    'region' => 'some-region',
                    'credentials' => 'some-credentials',
                    'host' => '',
                    'database' => '',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'bigquery',
            'expectedBackendSize' => null,
            'expectedCredentials' => [
                'schema' => 'some-schema',
                'region' => 'some-region',
                'credentials' => 'some-credentials',
            ],
        ];
        yield 'exasol' => [
            'input' => [
                'id' => '123456',
                'backendSize' => null,
                'connection' => [
                    'backend' => 'exasol',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'user' => 'some-user',
                    'schema' => 'some-schema',
                    'password' => 'some-secret',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'exasol',
            'expectedBackendSize' => null,
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'some-secret',
            ],
        ];
        yield 'redshift' => [
            'input' => [
                'id' => '123456',
                'backendSize' => null,
                'connection' => [
                    'backend' => 'redshift',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'user' => 'some-user',
                    'schema' => 'some-schema',
                    'password' => 'some-secret',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'redshift',
            'expectedBackendSize' => null,
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'some-secret',
            ],
        ];
        yield 'snowflake-large' => [
            'input' => [
                'id' => '123456',
                'backendSize' => 'large',
                'connection' => [
                    'backend' => 'snowflake',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'user' => 'some-user',
                    'schema' => 'some-schema',
                    'password' => 'some-secret',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'snowflake',
            'expectedBackendSize' => 'large',
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'some-secret',
                'privateKey' => null,
                'account' => 'some-host',
            ],
        ];
        yield 'snowflake-small' => [
            'input' => [
                'id' => '123456',
                'backendSize' => 'small',
                'connection' => [
                    'backend' => 'snowflake',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'user' => 'some-user',
                    'schema' => 'some-schema',
                    'password' => 'some-secret',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'snowflake',
            'expectedBackendSize' => 'small',
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'some-secret',
                'privateKey' => null,
                'account' => 'some-host',
            ],
        ];
        yield 'snowflake-sso' => [
            'input' => [
                'id' => '123456',
                'backendSize' => 'small',
                'connection' => [
                    'backend' => 'snowflake',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'user' => 'some-user',
                    'schema' => 'some-schema',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'snowflake',
            'expectedBackendSize' => 'small',
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => null,
                'privateKey' => null,
                'account' => 'some-host',
            ],
        ];
        yield 'synapse' => [
            'input' => [
                'id' => '123456',
                'backendSize' => null,
                'connection' => [
                    'backend' => 'synapse',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'user' => 'some-user',
                    'schema' => 'some-schema',
                    'password' => 'some-secret',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'synapse',
            'expectedBackendSize' => null,
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'some-secret',
            ],
        ];
        yield 'teradata' => [
            'input' => [
                'id' => '123456',
                'backendSize' => null,
                'connection' => [
                    'backend' => 'teradata',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'user' => 'some-user',
                    'schema' => 'some-schema',
                    'password' => 'some-secret',
                ],
            ],
            'expectedId' => '123456',
            'expectedBackend' => 'teradata',
            'expectedBackendSize' => null,
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'some-secret',
            ],
        ];
    }

    public function testFromInvalidDataArray(): void
    {
        $input = [
            'id' => '123456',
            'backendSize' => null,
            'connection' => [
                'backend' => 'invalid',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'password' => 'some-secret',
            ],
        ];
        $this->expectExceptionMessage('Invalid workspace connection data: Unsupported backend "invalid"');
        $this->expectException(StagingProviderException::class);
        StorageApiWorkspace::fromDataArray($input);
    }

    public function testFromBrokenDataArray(): void
    {
        $input = [
            'id' => '123456',
            'backendSize' => null,
            'connection' => [
                'backend' => 'snowflake',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'password' => 'some-secret',
            ],
        ];
        $this->expectExceptionMessage('Invalid workspace connection data: Undefined array key "host"');
        $this->expectException(StagingProviderException::class);
        StorageApiWorkspace::fromDataArray($input);
    }

    /** @dataProvider provideSnowflakeAccountTestData */
    public function testGetCredentialsWithAccount(string $host, string $expectedAccount): void
    {
        $input = [
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'host' => $host,
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'password' => 'some-secret',
            ],
        ];

        $workspace = StorageApiWorkspace::fromDataArray($input);
        self::assertArrayHasKey('account', $workspace->credentials);
        self::assertSame($expectedAccount, $workspace->credentials['account']);
    }

    public function provideSnowflakeAccountTestData(): iterable
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

        yield 'us-east-1.global.snowflakecomputing.com' => [
            'host' => 'us-east-1.global.snowflakecomputing.com',
            'account' => 'us-east',
        ];
    }
}
