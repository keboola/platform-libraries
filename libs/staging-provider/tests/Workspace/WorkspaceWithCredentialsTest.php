<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Workspace;

use Keboola\StagingProvider\Exception\StagingProviderException;
use Keboola\StagingProvider\Workspace\WorkspaceWithCredentials;
use Keboola\StorageApi\WorkspaceLoginType;
use PHPUnit\Framework\TestCase;

class WorkspaceWithCredentialsTest extends TestCase
{
    /** @dataProvider provideCredentialsData */
    public function testFromDataArray(
        array $data,
        string $expectedId,
        string $expectedBackendType,
        string $expectedSize,
        WorkspaceLoginType $expectedLoginType,
        array $expectedCredentials
    ): void {
        $workspace = WorkspaceWithCredentials::createFromData($data);

        self::assertSame($expectedId, $workspace->getWorkspaceId());
        self::assertSame($expectedBackendType, $workspace->getBackendType());
        self::assertSame($expectedSize, $workspace->getBackendSize());
        self::assertSame($expectedLoginType, $workspace->getLoginType());
        self::assertSame($expectedCredentials, $workspace->getCredentials());
    }

    public static function provideCredentialsData(): iterable
    {
        yield 'small bigquery' => [
            'data' => [
                'id' => '123456',
                'backendSize' => 'small',
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
            'expectedBackendType' => 'bigquery',
            'expectedBackendSize' => 'small',
            'expectedLoginType' => WorkspaceLoginType::DEFAULT,
            'expectedCredentials' => [
                'schema' => 'some-schema',
                'region' => 'some-region',
                'credentials' => 'some-credentials',
            ],
        ];

        yield 'snowflake' => [
            'data' => [
                'id' => '479',
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
            'expectedId' => '479',
            'expectedBackendType' => 'snowflake',
            'expectedBackendSize' => 'large',
            'expectedLoginType' => WorkspaceLoginType::DEFAULT,
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
            'data' => [
                'id' => '987',
                'backendSize' => 'medium',
                'connection' => [
                    'backend' => 'snowflake',
                    'loginType' => 'snowflake-person-sso',
                    'host' => 'some-host',
                    'warehouse' => 'some-warehouse',
                    'database' => 'some-database',
                    'user' => 'some-user',
                    'schema' => 'some-schema',
                ],
            ],
            'expectedId' => '987',
            'expectedBackendType' => 'snowflake',
            'expectedBackendSize' => 'medium',
            'expectedLoginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_SSO,
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
    }

    public function testFromInvalidDataArray(): void
    {
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid workspace data: ');

        WorkspaceWithCredentials::createFromData([]);
    }

    public function testFromDataWithUnsupportedBackend(): void
    {
        $this->expectException(StagingProviderException::class);
        $this->expectExceptionMessage('Invalid workspace data: Unsupported backend "invalid"');

        WorkspaceWithCredentials::createFromData([
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'invalid',
            ],
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

        $workspace = WorkspaceWithCredentials::createFromData($input);

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

    public function testSnowflakeWithPrivateKey(): void
    {
        $input = [
            'id' => '123456',
            'backendSize' => 'small',
            'connection' => [
                'backend' => 'snowflake',
                'loginType' => WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR->value,
                'host' => 'some-host',
                'warehouse' => 'some-warehouse',
                'database' => 'some-database',
                'user' => 'some-user',
                'schema' => 'some-schema',
                'privateKey' => 'some-private-key',
            ],
        ];

        $workspace = WorkspaceWithCredentials::createFromData($input);

        self::assertSame(WorkspaceLoginType::SNOWFLAKE_SERVICE_KEYPAIR, $workspace->getLoginType());
        self::assertSame('some-private-key', $workspace->getCredentials()['privateKey'] ?? null);
        self::assertNull($workspace->getCredentials()['password'] ?? null);
    }

}
