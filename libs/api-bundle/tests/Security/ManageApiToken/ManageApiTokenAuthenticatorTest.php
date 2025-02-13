<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\ManageApiToken;

use Generator;
use Keboola\ApiBundle\Attribute\ManageApiTokenAuth;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiClientFactory;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiToken;
use Keboola\ApiBundle\Security\ManageApiToken\ManageApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

class ManageApiTokenAuthenticatorTest extends TestCase
{
    /**
     * @dataProvider provideAuthorizeTokenSuccessData
     */
    public function testAuthorizeTokenSuccess(ManageApiTokenAuth $authAttribute, ManageApiToken $token): void
    {

        $authenticator = new ManageApiTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $authenticator->authorizeToken($authAttribute, $token);

        $this->expectNotToPerformAssertions();
    }

    /**
     * @dataProvider provideAuthorizeTokenExceptionsData
     */
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

    public function provideAuthorizeTokenSuccessData(): Generator
    {
        yield 'scope needed and provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => ['some:scope'],
            ]),
        ];

        yield 'super admin token needed and provided' => [
            new ManageApiTokenAuth(scopes: [], isSuperAdmin: true),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'user' => [
                    'id' => 3801,
                    'mfaEnabled' => true,
                    'isSuperAdmin' => true,
                ],
                'scopes' => [],
            ]),
        ];

        yield 'no scope or token needed' => [
            new ManageApiTokenAuth(),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => [],
            ]),
        ];

        yield 'two scopes needed and both provided' => [
                new ManageApiTokenAuth(scopes: ['some:scope', 'some:other-scope']),
                ManageApiToken::fromVerifyResponse([
                    'id' => 123,
                    'description' => 'some-description',
                    'scopes' => ['some:scope', 'some:other-scope'],
                ]),
        ];

        yield 'feature needed and provided' => [
            new ManageApiTokenAuth(features: ['can-manage']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => [],
                'user' => [
                    'features' => ['can-manage'],
                ],
            ]),
        ];
    }

    public function provideAuthorizeTokenExceptionsData(): Generator
    {
        yield 'scope needed and not provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => [],
            ]),
            'Authentication token is valid but missing following scopes: some:scope',
        ];

        yield 'scope needed and different one provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => ['some:other-scope'],
            ]),
            'Authentication token is valid but missing following scopes: some:scope',
        ];

        yield 'two scopes needed and only one provided' => [
            new ManageApiTokenAuth(scopes: ['some:scope', 'some:other-scope']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => ['some:scope'],
            ]),
            'Authentication token is valid but missing following scopes: some:other-scope',
        ];

        yield 'super admin token needed and not provided' => [
            new ManageApiTokenAuth(scopes: [], isSuperAdmin: true),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => [],
            ]),
            'Authentication token is not super admin',
        ];

        yield 'super admin token forbidden and provided' => [
            new ManageApiTokenAuth(scopes: [], isSuperAdmin: false),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => ['some:scope'],
                'user' => [
                    'id' => 3801,
                    'mfaEnabled' => true,
                    'isSuperAdmin' => true,
                ],
            ]),
            'Authentication token must not be super admin',
        ];

        yield 'feature required but no features provided' => [
            new ManageApiTokenAuth(features: ['can-manage']),
            ManageApiToken::fromVerifyResponse([
                'id' => 123,
                'description' => 'some-description',
                'scopes' => ['some:scope'],
                'user' => [
                    'id' => 3801,
                    'mfaEnabled' => true,
                    'isSuperAdmin' => true,
                    'features' => [],
                ],
            ]),
            'Authentication token is valid but missing following features: can-manage',
        ];
    }
}
