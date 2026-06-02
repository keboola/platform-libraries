<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\AuthBridge;

use Generator;
use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use Keboola\ApiBundle\AuthBridge\ResolvedStorageToken;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class ResolvedStorageTokenTest extends TestCase
{
    public function testFromResponseDataWithFullValidData(): void
    {
        $data = [
            'storageToken' => 'abc-legacy-token',
            'projectId' => 42,
            'tokenId' => 999,
            'userId' => 7,
            'expiresAt' => '2026-12-31T23:59:59Z',
        ];

        $token = ResolvedStorageToken::fromResponseData($data);

        self::assertSame('abc-legacy-token', $token->storageToken);
        self::assertSame(42, $token->projectId);
        self::assertSame('999', $token->tokenId);
        self::assertSame('7', $token->userId);
        self::assertSame('2026-12-31T23:59:59Z', $token->expiresAt);
    }

    public function testFromResponseDataWithNullExpiresAt(): void
    {
        $data = [
            'storageToken' => 'legacy-token-xyz',
            'projectId' => 1,
            'tokenId' => 'tok-1',
            'userId' => 'usr-1',
            'expiresAt' => null,
        ];

        $token = ResolvedStorageToken::fromResponseData($data);

        self::assertNull($token->expiresAt);
    }

    public function testFromResponseDataWithMissingExpiresAtKey(): void
    {
        $data = [
            'storageToken' => 'legacy-token-xyz',
            'projectId' => 1,
            'tokenId' => 'tok-1',
            'userId' => 'usr-1',
        ];

        $token = ResolvedStorageToken::fromResponseData($data);

        self::assertNull($token->expiresAt);
    }

    public function testFromResponseDataCoercesScalarTokenIdAndUserId(): void
    {
        $data = [
            'storageToken' => 'some-token',
            'projectId' => 10,
            'tokenId' => 123,
            'userId' => 456,
            'expiresAt' => null,
        ];

        $token = ResolvedStorageToken::fromResponseData($data);

        self::assertSame('123', $token->tokenId);
        self::assertSame('456', $token->userId);
        self::assertSame(10, $token->projectId);
    }

    /**
     * @param array<mixed> $data
     * @param class-string<\Throwable> $expectedException
     */
    #[DataProvider('provideInvalidResponseData')]
    public function testFromResponseDataThrowsOnInvalidData(
        array $data,
        string $expectedException,
        string $expectedMessageContains,
    ): void {
        self::expectException($expectedException);
        self::expectExceptionMessage($expectedMessageContains);

        ResolvedStorageToken::fromResponseData($data);
    }

    public static function provideInvalidResponseData(): Generator
    {
        yield 'missing storageToken key' => [
            'data' => [
                'projectId' => 1,
                'tokenId' => 'tok',
                'userId' => 'usr',
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"storageToken"',
        ];

        yield 'empty storageToken string' => [
            'data' => [
                'storageToken' => '',
                'projectId' => 1,
                'tokenId' => 'tok',
                'userId' => 'usr',
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"storageToken"',
        ];

        yield 'non-int projectId (string)' => [
            'data' => [
                'storageToken' => 'tok',
                'projectId' => '42',
                'tokenId' => 'tok',
                'userId' => 'usr',
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"projectId"',
        ];

        yield 'missing projectId key' => [
            'data' => [
                'storageToken' => 'tok',
                'tokenId' => 'tok',
                'userId' => 'usr',
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"projectId"',
        ];

        yield 'missing tokenId key' => [
            'data' => [
                'storageToken' => 'tok',
                'projectId' => 1,
                'userId' => 'usr',
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"tokenId"',
        ];

        yield 'non-scalar tokenId (array)' => [
            'data' => [
                'storageToken' => 'tok',
                'projectId' => 1,
                'tokenId' => ['nested'],
                'userId' => 'usr',
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"tokenId"',
        ];

        yield 'missing userId key' => [
            'data' => [
                'storageToken' => 'tok',
                'projectId' => 1,
                'tokenId' => 'tok',
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"userId"',
        ];

        yield 'non-scalar userId (array)' => [
            'data' => [
                'storageToken' => 'tok',
                'projectId' => 1,
                'tokenId' => 'tok',
                'userId' => ['nested'],
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"userId"',
        ];

        yield 'non-string expiresAt (int)' => [
            'data' => [
                'storageToken' => 'tok',
                'projectId' => 1,
                'tokenId' => 'tok',
                'userId' => 'usr',
                'expiresAt' => 12345,
            ],
            'expectedException' => StorageTokenResolverException::class,
            'expectedMessageContains' => '"expiresAt"',
        ];
    }
}
