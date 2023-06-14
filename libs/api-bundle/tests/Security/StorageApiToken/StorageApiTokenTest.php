<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use PHPUnit\Framework\TestCase;

class StorageApiTokenTest extends TestCase
{
    public function testAccessors(): void
    {
        $token = new StorageApiToken(
            [
                'id' => '123',
                'description' => 'token description',
                'owner' => [
                    'id' => '456',
                    'name' => 'my project',
                    'features' => ['foo', 'bar'],
                    'payAsYouGo' => [
                        'purchasedCredits' => 1.23,
                    ],
                    'fileStorageProvider' => 'aws',
                ],
                'admin' => [
                    'samlParameters' => [
                        'userId' => '789',
                    ],
                    'role' => 'admin',
                ],
            ],
            'tokenValue',
        );

        self::assertSame('123', $token->getTokenId());
        self::assertSame('tokenValue', $token->getTokenValue());
        self::assertSame('456', $token->getProjectId());
        self::assertSame(['foo', 'bar'], $token->getFeatures());
        self::assertTrue($token->hasFeature('foo'));
        self::assertFalse($token->hasFeature('baz'));
        self::assertSame(1.23, $token->getPayAsYouGoPurchasedCredits());
        self::assertSame('789', $token->getSamlUserId());
        self::assertSame('123', $token->getUserIdentifier());
        self::assertSame('aws', $token->getFileStorageProvider());
        self::assertSame('my project', $token->getProjectName());
        self::assertSame('token description', $token->getTokenDesc());
        self::assertSame(['admin'], $token->getRoles());

        self::assertSame(
            [
                'id' => '123',
                'description' => 'token description',
                'owner' => [
                    'id' => '456',
                    'name' => 'my project',
                    'features' => ['foo', 'bar'],
                    'payAsYouGo' => [
                        'purchasedCredits' => 1.23,
                    ],
                    'fileStorageProvider' => 'aws',
                ],
                'admin' => [
                    'samlParameters' => [
                        'userId' => '789',
                    ],
                    'role' => 'admin',
                ],
            ],
            $token->getTokenInfo(),
        );
    }

    public function testNoRoles(): void
    {
        $token = new StorageApiToken(
            [
                'id' => '123',
                'description' => 'token description',
                'owner' => [
                    'id' => '456',
                    'name' => 'my project',
                    'features' => ['foo', 'bar'],
                    'payAsYouGo' => [
                        'purchasedCredits' => 1.23,
                    ],
                    'fileStorageProvider' => 'aws',
                ],
                'admin' => [
                    'samlParameters' => [
                        'userId' => '789',
                    ],
                ],
            ],
            'tokenValue',
        );

        self::assertSame([], $token->getRoles());
    }
}
