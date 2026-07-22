<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\ApplicationToken;

use Generator;
use Keboola\ApiBundle\Attribute\ApplicationTokenAuth;
use Keboola\ApiBundle\Security\ApplicationToken\ApplicationToken;
use Keboola\ApiBundle\Security\ApplicationToken\ApplicationTokenAuthenticator;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Keboola\ManageApi\Client as ManageApiClient;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

class ApplicationTokenAuthenticatorTest extends TestCase
{
    #[DataProvider('provideAuthorizeTokenSuccessData')]
    public function testAuthorizeTokenSuccess(
        ApplicationTokenAuth $authAttribute,
        ApplicationToken $token,
    ): void {

        $authenticator = new ApplicationTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $authenticator->authorizeToken($authAttribute, $token);

        $this->expectNotToPerformAssertions();
    }

    #[DataProvider('provideAuthorizeTokenExceptionsData')]
    public function testAuthorizeTokenExceptions(
        ApplicationTokenAuth $authAttribute,
        ApplicationToken $token,
        string $expectedMessage,
    ): void {
        $authenticator = new ApplicationTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $this->expectException(AccessDeniedException::class);
        $this->expectExceptionMessage($expectedMessage);

        $authenticator->authorizeToken($authAttribute, $token);
    }

    public static function provideAuthorizeTokenSuccessData(): Generator
    {
        yield 'scope needed and provided' => [
            new ApplicationTokenAuth(scopes: ['some:scope']),
            ApplicationToken::fromVerifyResponse([
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
            new ApplicationTokenAuth(scopes: [], isSuperAdmin: true),
            ApplicationToken::fromVerifyResponse([
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
            new ApplicationTokenAuth(),
            ApplicationToken::fromVerifyResponse([
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
            new ApplicationTokenAuth(scopes: ['some:scope', 'some:other-scope']),
            ApplicationToken::fromVerifyResponse([
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
            new ApplicationTokenAuth(scopes: ['some:scope']),
            ApplicationToken::fromVerifyResponse([
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
            new ApplicationTokenAuth(scopes: ['some:scope']),
            ApplicationToken::fromVerifyResponse([
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
            new ApplicationTokenAuth(scopes: ['some:scope', 'some:other-scope']),
            ApplicationToken::fromVerifyResponse([
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
            new ApplicationTokenAuth(scopes: [], isSuperAdmin: true),
            ApplicationToken::fromVerifyResponse([
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
            new ApplicationTokenAuth(scopes: [], isSuperAdmin: false),
            ApplicationToken::fromVerifyResponse([
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
        $authenticator = new ApplicationTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', 'my-manage-token');

        self::assertSame('my-manage-token', $authenticator->extractCredential($request));
    }

    public function testExtractTokenReturnsNullWhenNoHeader(): void
    {
        $authenticator = new ApplicationTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');

        self::assertNull($authenticator->extractCredential($request));
    }

    public function testExtractTokenReturnsServiceAccountHeaderVerbatim(): void
    {
        $authenticator = new ApplicationTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-Kubernetes-Authorization', 'Bearer my-jwt-token');

        // The Bearer scheme is kept here; it is stripped in authenticateToken.
        self::assertSame('Bearer my-jwt-token', $authenticator->extractCredential($request));
    }

    public function testExtractTokenPrefersManageHeaderOverServiceAccountHeader(): void
    {
        $authenticator = new ApplicationTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', 'my-manage-token');
        $request->headers->set('X-Kubernetes-Authorization', 'Bearer my-jwt-token');

        self::assertSame('my-manage-token', $authenticator->extractCredential($request));
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
            ->method('getClientForManageToken')
            ->with('my-manage-token')
            ->willReturn($manageApiClient);
        $clientFactory->expects(self::never())
            ->method('getClientForServiceAccountToken');

        $authenticator = new ApplicationTokenAuthenticator($clientFactory);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', 'my-manage-token');

        $token = $authenticator->authenticateToken(
            new ApplicationTokenAuth(scopes: ['some:scope']),
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
            ->method('getClientForServiceAccountToken')
            ->with('my-jwt-token')
            ->willReturn($manageApiClient);
        $clientFactory->expects(self::never())
            ->method('getClientForManageToken');

        $authenticator = new ApplicationTokenAuthenticator($clientFactory);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-Kubernetes-Authorization', 'Bearer my-jwt-token');

        $token = $authenticator->authenticateToken(
            new ApplicationTokenAuth(scopes: ['some:scope']),
            'Bearer my-jwt-token',
            $request,
        );

        self::assertSame(['some:scope'], $token->getScopes());
    }

    public function testAuthenticateTokenRejectsServiceAccountHeaderWithoutBearerScheme(): void
    {
        $clientFactory = $this->createMock(ManageApiClientFactory::class);
        $clientFactory->expects(self::never())->method('getClientForServiceAccountToken');
        $clientFactory->expects(self::never())->method('getClientForManageToken');

        $authenticator = new ApplicationTokenAuthenticator($clientFactory);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-Kubernetes-Authorization', 'my-jwt-token');

        $this->expectException(CustomUserMessageAuthenticationException::class);
        $this->expectExceptionMessage('Invalid X-Kubernetes-Authorization header: expected "Bearer <token>"');

        $authenticator->authenticateToken(
            new ApplicationTokenAuth(scopes: ['some:scope']),
            'my-jwt-token',
            $request,
        );
    }

    public function testExtractTokenReturnsEmptyManageHeaderVerbatim(): void
    {
        $authenticator = new ApplicationTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', '');

        // An empty Manage header is still a present-but-invalid value; extractCredential returns it
        // verbatim and the emptiness is rejected later in authenticateToken.
        self::assertSame('', $authenticator->extractCredential($request));
    }

    public function testEmptyManageHeaderTakesPrecedenceOverServiceAccountHeader(): void
    {
        $authenticator = new ApplicationTokenAuthenticator(
            $this->createMock(ManageApiClientFactory::class),
        );

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', '');
        $request->headers->set('X-Kubernetes-Authorization', 'Bearer my-jwt-token');

        // Whichever header the caller chose to send wins, even when empty — sending both is a
        // caller error, but we don't try to "fix it up" by silently falling back.
        self::assertSame('', $authenticator->extractCredential($request));
    }

    public function testAuthenticateTokenRejectsEmptyManageHeader(): void
    {
        $clientFactory = $this->createMock(ManageApiClientFactory::class);
        $clientFactory->expects(self::never())->method('getClientForManageToken');
        $clientFactory->expects(self::never())->method('getClientForServiceAccountToken');

        $authenticator = new ApplicationTokenAuthenticator($clientFactory);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ManageApiToken', '');

        $this->expectException(CustomUserMessageAuthenticationException::class);
        $this->expectExceptionMessage('Invalid X-KBC-ManageApiToken header: token must not be empty');

        $authenticator->authenticateToken(
            new ApplicationTokenAuth(scopes: ['some:scope']),
            '',
            $request,
        );
    }

    public function testAuthenticateTokenRejectsEmptyServiceAccountHeader(): void
    {
        $clientFactory = $this->createMock(ManageApiClientFactory::class);
        $clientFactory->expects(self::never())->method('getClientForServiceAccountToken');
        $clientFactory->expects(self::never())->method('getClientForManageToken');

        $authenticator = new ApplicationTokenAuthenticator($clientFactory);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-Kubernetes-Authorization', '');

        $this->expectException(CustomUserMessageAuthenticationException::class);
        $this->expectExceptionMessage('Invalid X-Kubernetes-Authorization header: expected "Bearer <token>"');

        $authenticator->authenticateToken(
            new ApplicationTokenAuth(scopes: ['some:scope']),
            '',
            $request,
        );
    }

    public function testAuthenticateTokenRejectsServiceAccountHeaderWithEmptyTokenAfterBearer(): void
    {
        $clientFactory = $this->createMock(ManageApiClientFactory::class);
        $clientFactory->expects(self::never())->method('getClientForServiceAccountToken');
        $clientFactory->expects(self::never())->method('getClientForManageToken');

        $authenticator = new ApplicationTokenAuthenticator($clientFactory);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-Kubernetes-Authorization', 'Bearer ');

        $this->expectException(CustomUserMessageAuthenticationException::class);
        $this->expectExceptionMessage('Invalid X-Kubernetes-Authorization header: token must not be empty');

        $authenticator->authenticateToken(
            new ApplicationTokenAuth(scopes: ['some:scope']),
            'Bearer ',
            $request,
        );
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
