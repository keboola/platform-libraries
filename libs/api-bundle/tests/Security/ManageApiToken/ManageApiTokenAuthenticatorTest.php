<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\ManageApiToken;

use Generator;
use Keboola\ApiBundle\Attribute\ManageApiTokenAuth;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiToken;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiTokenAuthenticator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

class ManageApiTokenAuthenticatorTest extends TestCase
{
    #[DataProvider('provideAuthorizeTokenSuccessData')]
    public function testAuthorizeTokenSuccess(ManageApiTokenAuth $authAttribute, ManageApiToken $token): void
    {

        $authenticator = new ManageApiTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $authenticator->authorizeToken($authAttribute, $token);

        $this->expectNotToPerformAssertions();
    }

    #[DataProvider('provideAuthorizeTokenExceptionsData')]
    public function testAuthorizeTokenExceptions(
        ManageApiTokenAuth $authAttribute,
        ManageApiToken $token,
        string $expectedMessage,
    ): void {
        $authenticator = new ManageApiTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $this->expectException(AccessDeniedException::class);
        $this->expectExceptionMessage($expectedMessage);

        $authenticator->authorizeToken($authAttribute, $token);
    }

    public static function provideAuthorizeTokenSuccessData(): Generator
    {
        yield 'scope needed and provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => ['some:scope'],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => false,
                ],
            ]),
        ];

        yield 'super admin token needed and provided' => [
            new ManageApiTokenAuth(scopes: [], isSuperAdmin: true),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => [],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => true,
                ],
            ]),
        ];

        yield 'no scope or token needed' => [
            new ManageApiTokenAuth(),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => [],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => false,
                ],
            ]),
        ];

        yield 'two scopes needed and both provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope', 'some:other-scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => ['some:scope', 'some:other-scope'],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => false,
                ],
            ]),
        ];
    }

    public static function provideAuthorizeTokenExceptionsData(): Generator
    {
        yield 'scope needed and not provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => [],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => false,
                ],
            ]),
            'Authentication token is valid but missing following scopes: some:scope',
        ];

        yield 'scope needed and different one provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => ['some:other-scope'],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => false,
                ],
            ]),
            'Authentication token is valid but missing following scopes: some:scope',
        ];

        yield 'two scopes needed and only one provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope', 'some:other-scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => ['some:scope'],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => false,
                ],
            ]),
            'Authentication token is valid but missing following scopes: some:other-scope',
        ];

        yield 'super admin token needed and not provided' => [
            new ManageApiTokenAuth(scopes: [], isSuperAdmin: true),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => [],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => false,
                ],
            ]),
            'Authentication token is not super admin',
        ];

        yield 'super admin token forbidden and provided' => [
            new ManageApiTokenAuth(scopes: [], isSuperAdmin: false),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'created' => '2024-03-21T12:26:43+0100',
                'lastUsed' => '2024-03-21T12:26:54+0100',
                'expires' => '2024-03-21T13:26:43+0100',
                'isSessionToken' => false,
                'isExpired' => false,
                'isDisabled' => false,
                'scopes' => ['some:scope'],
                'type' => 'admin',
                'creator' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                ],
                'user' => [
                    'id' => 3801,
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                    'mfaEnabled' => true,
                    'features' => [],
                    'canAccessLogs' => true,
                    'isSuperAdmin' => true,
                ],
            ]),
            'Authentication token must not be super admin',
        ];
    }
}
