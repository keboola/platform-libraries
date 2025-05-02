<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Provider\Workspace;
use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class WorkspaceTest extends TestCase
{
    public function testFromDataArray(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'abs',
                'container' => 'some-host',
                'connectionString' => 'some-warehouse',
            ],
        ]);

        self::assertSame('123456', $workspace->getId());
        self::assertSame('abs', $workspace->getBackendType());
        self::assertSame('small', $workspace->getBackendSize());
        self::assertFalse($workspace->hasCredentials());
    }

    public function testAccessingCredentialsBeforeInitializationThrowsError(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'abs',
                'container' => 'some-host',
                'connectionString' => 'some-warehouse',
            ],
        ]);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Credentials are not available');

        $workspace->getCredentials();
    }

    public function testFromInvalidDataArray(): void
    {
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid workspace data: ');

        Workspace::createFromData([]);
    }

    public static function provideCredentialsData(): iterable
    {
        yield 'abs' => [
            'backendType' => 'abs',
            'loginType' => WorkspaceLoginType::DEFAULT,
            'connectionData' => [
                'backend' => 'abs',
                'container' => 'some-host',
                'connectionString' => 'some-warehouse',
            ],
            'expectedCredentials' => [
                'container' => 'some-host',
                'connectionString' => 'some-warehouse',
            ],
        ];

        yield 'bigquery' => [
            'backendType' => 'bigquery',
            'loginType' => WorkspaceLoginType::DEFAULT,
            'connectionData' => [
                'backend' => 'bigquery',
                'schema' => 'some-schema',
                'region' => 'some-region',
                'credentials' => 'some-credentials',
                'host' => '',
                'database' => '',
            ],
            'expectedCredentials' => [
                'schema' => 'some-schema',
                'region' => 'some-region',
                'credentials' => 'some-credentials',
            ],
        ];

        yield 'exasol' => [
            'backendType' => 'exasol',
            'loginType' => WorkspaceLoginType::DEFAULT,
            'connectionData' => [
                'backend' => 'exasol',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'password' => 'some-secret',
            ],
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
            'backendType' => 'redshift',
            'loginType' => WorkspaceLoginType::DEFAULT,
            'connectionData' => [
                'backend' => 'redshift',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'password' => 'some-secret',
            ],
            'expectedCredentials' => [
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'some-secret',
            ],
        ];

        yield 'snowflake' => [
            'backendType' => 'snowflake',
            'loginType' => WorkspaceLoginType::DEFAULT,
            'connectionData' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'password' => 'some-secret',
            ],
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
            'backendType' => 'snowflake',
            'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_SSO,
            'connectionData' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
            ],
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
            'backendType' => 'synapse',
            'loginType' => WorkspaceLoginType::DEFAULT,
            'connectionData' => [
                'backend' => 'synapse',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'password' => 'some-secret',
            ],
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
            'backendType' => 'teradata',
            'loginType' => WorkspaceLoginType::DEFAULT,
            'connectionData' => [
                'backend' => 'teradata',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'password' => 'some-secret',
            ],
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

    /** @dataProvider provideCredentialsData */
    public function testSetCredentials(
        string $backendType,
        WorkspaceLoginType $loginType,
        array $connectionData,
        array $expectedCredentials,
    ): void {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => $connectionData,
        ]);
        $workspace->setCredentialsFromData($connectionData);

        self::assertSame($expectedCredentials, $workspace->getCredentials());
    }

    public function testSetCredentialsWithInvalidData(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
            ],
        ]);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage(
            'Invalid credentials data: Keboola\StagingProvider\Provider\Workspace::parseSnowflakeAccount(): ' .
            'Argument #1 ($host) must be of type string, null given',
        );

        // Override all connection data with invalid data
        $workspace->setCredentialsFromData([
            'host' => null,
            'warehouse' => null,
            'database' => null,
            'schema' => null,
            'user' => null,
            'password' => null,
        ]);
    }

    /** @dataProvider provideSnowflakeAccountTestData */
    public function testSetSnowflakeAccountParsing(string $host, string $expectedAccount): void
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

        $workspace = Workspace::createFromData($input);
        $workspace->setCredentialsFromData($input['connection']);

        self::assertIsArray($workspace->getCredentials());
        self::assertArrayHasKey('account', $workspace->getCredentials());
        self::assertSame($expectedAccount, $workspace->getCredentials()['account']);
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

    public function testHasCredentials(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
            ],
        ]);

        self::assertFalse($workspace->hasCredentials());

        $workspace->setCredentialsFromData([
            'backend' => 'snowflake',
            'host' => 'some-host',
            'warehouse' => 'some-warehouse',
            'database' => 'some-database',
            'schema' => 'some-schema',
            'user' => 'some-user',
            'password' => 'some-secret',
        ]);

        self::assertTrue($workspace->hasCredentials());
    }

    public function testGetLoginType(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
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
        ]);

        self::assertSame(WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR, $workspace->getLoginType());
    }

    public function testInvalidBackendType(): void
    {
        $workspace = Workspace::createFromData([
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'invalid',
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'schema' => 'some-schema',
                'user' => 'some-user',
                'password' => 'some-secret',
            ],
        ]);

        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Unsupported backend "invalid"');

        $workspace->setCredentialsFromData([
            'host' => 'some-host',
            'warehouse' => 'some-warehouse',
            'database' => 'some-database',
            'schema' => 'some-schema',
            'user' => 'some-user',
            'password' => 'some-secret',
        ]);
    }
}
