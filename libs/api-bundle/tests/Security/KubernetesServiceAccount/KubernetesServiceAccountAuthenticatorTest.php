<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\KubernetesServiceAccount;

use Generator;
use Keboola\ApiBundle\Attribute\KubernetesServiceAccountAuth;
use Keboola\ApiBundle\Security\KubernetesServiceAccount\KubernetesServiceAccountAuthenticator;
use Keboola\ApiBundle\Security\KubernetesServiceAccount\KubernetesServiceAccountToken;
use Keboola\ApiBundle\Security\KubernetesServiceAccount\ManageApiClientFactory;
use Keboola\ManageApi\Client as ManageApiClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

class KubernetesServiceAccountAuthenticatorTest extends TestCase
{
    #[DataProvider('provideAuthorizeTokenSuccessData')]
    public function testAuthorizeTokenSuccess(
        KubernetesServiceAccountAuth $authAttribute,
        KubernetesServiceAccountToken $token,
    ): void {

        $authenticator = new KubernetesServiceAccountAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $authenticator->authorizeToken($authAttribute, $token);

        $this->expectNotToPerformAssertions();
    }

    #[DataProvider('provideAuthorizeTokenExceptionsData')]
    public function testAuthorizeTokenExceptions(
        KubernetesServiceAccountAuth $authAttribute,
        KubernetesServiceAccountToken $token,
        string $expectedMessage,
    ): void {
        $authenticator = new KubernetesServiceAccountAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $this->expectException(AccessDeniedException::class);
        $this->expectExceptionMessage($expectedMessage);

        $authenticator->authorizeToken($authAttribute, $token);
    }

    public static function provideAuthorizeTokenSuccessData(): Generator
    {
        yield 'scope needed and provided' => [
            new KubernetesServiceAccountAuth(scopes: ['some:scope']),
            KubernetesServiceAccountToken::fromVerifyResponse([
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
            new KubernetesServiceAccountAuth(scopes: [], isSuperAdmin: true),
            KubernetesServiceAccountToken::fromVerifyResponse([
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
            new KubernetesServiceAccountAuth(),
            KubernetesServiceAccountToken::fromVerifyResponse([
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
            new KubernetesServiceAccountAuth(scopes: ['some:scope', 'some:other-scope']),
            KubernetesServiceAccountToken::fromVerifyResponse([
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
            new KubernetesServiceAccountAuth(scopes: ['some:scope']),
            KubernetesServiceAccountToken::fromVerifyResponse([
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
            new KubernetesServiceAccountAuth(scopes: ['some:scope']),
            KubernetesServiceAccountToken::fromVerifyResponse([
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
            new KubernetesServiceAccountAuth(scopes: ['some:scope', 'some:other-scope']),
            KubernetesServiceAccountToken::fromVerifyResponse([
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
            new KubernetesServiceAccountAuth(scopes: [], isSuperAdmin: true),
            KubernetesServiceAccountToken::fromVerifyResponse([
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
            new KubernetesServiceAccountAuth(scopes: [], isSuperAdmin: false),
            KubernetesServiceAccountToken::fromVerifyResponse([
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

    public function testExtractTokenFromHeader(): void
    {
        $authenticator = new KubernetesServiceAccountAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', 'my-manage-token');

        self::assertSame('my-manage-token', $authenticator->extractToken($request));
    }

    public function testExtractTokenReturnsNullWhenNoHeader(): void
    {
        $authenticator = new KubernetesServiceAccountAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');

        self::assertNull($authenticator->extractToken($request));
    }

    public function testExtractTokenStripsBearerFromServiceAccountHeader(): void
    {
        $authenticator = new KubernetesServiceAccountAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-Kubernetes-Authorization', 'Bearer my-jwt-token');

        self::assertSame('my-jwt-token', $authenticator->extractToken($request));
    }

    public function testExtractTokenPrefersManageHeaderOverServiceAccountHeader(): void
    {
        $authenticator = new KubernetesServiceAccountAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', 'my-manage-token');
        $request->headers->set('X-Kubernetes-Authorization', 'Bearer my-jwt-token');

        self::assertSame('my-manage-token', $authenticator->extractToken($request));
    }

    public function testAuthenticateTokenUsesManageClientForManageHeader(): void
    {
        $verifyResponse = self::verifyResponse(['some:scope'], false);

        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient->expects(self::once())
            ->method('verifyToken')
            ->willReturn($verifyResponse);

        $clientFactory = $this->createMock(ManageApiClientFactory::class);
        $clientFactory->expects(self::once())
            ->method('getClient')
            ->with('my-manage-token')
            ->willReturn($manageApiClient);
        $clientFactory->expects(self::never())
            ->method('getClientForJwt');

        $authenticator = new KubernetesServiceAccountAuthenticator($clientFactory);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', 'my-manage-token');

        $token = $authenticator->authenticateToken(
            new KubernetesServiceAccountAuth(scopes: ['some:scope']),
            'my-manage-token',
            $request,
        );

        self::assertSame(['some:scope'], $token->getScopes());
    }

    public function testAuthenticateTokenUsesJwtClientForServiceAccountHeader(): void
    {
        $verifyResponse = self::verifyResponse(['some:scope'], false);

        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient->expects(self::once())
            ->method('verifyToken')
            ->willReturn($verifyResponse);

        $clientFactory = $this->createMock(ManageApiClientFactory::class);
        $clientFactory->expects(self::once())
            ->method('getClientForJwt')
            ->with('my-jwt-token')
            ->willReturn($manageApiClient);
        $clientFactory->expects(self::never())
            ->method('getClient');

        $authenticator = new KubernetesServiceAccountAuthenticator($clientFactory);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-Kubernetes-Authorization', 'Bearer my-jwt-token');

        $token = $authenticator->authenticateToken(
            new KubernetesServiceAccountAuth(scopes: ['some:scope']),
            'my-jwt-token',
            $request,
        );

        self::assertSame(['some:scope'], $token->getScopes());
    }

    /**
     * @param list<string> $scopes
     * @return array<string, mixed>
     */
    private static function verifyResponse(array $scopes, bool $isSuperAdmin): array
    {
        return [
            'id' => 123,
            'description' => 'some-description',
            'created' => '2024-03-21T12:26:43+0100',
            'lastUsed' => '2024-03-21T12:26:54+0100',
            'expires' => '2024-03-21T13:26:43+0100',
            'isSessionToken' => false,
            'isExpired' => false,
            'isDisabled' => false,
            'scopes' => $scopes,
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
                'isSuperAdmin' => $isSuperAdmin,
            ],
        ];
    }
}
