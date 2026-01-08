<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security;

use Keboola\ApiBundle\Attribute\ManageApiTokenAuth;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use Keboola\ApiBundle\Security\AttributeAuthenticator;
use Keboola\ApiBundle\Security\MultiHeaderTokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenAuthenticatorInterface;
use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\ApiBundle\Util\ControllerReflector;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\Container;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;
use Symfony\Component\Security\Core\Exception\AuthenticationException;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

// disable some PHPCS rules to make the anonymous controller class more readable
// phpcs:disable Squiz.Functions.MultiLineFunctionDeclaration.BraceOnSameLine
// phpcs:disable Squiz.WhiteSpace.ScopeClosingBrace.ContentBefore
// phpcs:disable Generic.WhiteSpace.ScopeIndent.IncorrectExact
class AttributeAuthenticatorTest extends TestCase
{
    public static function provideSupportsRequestTestData(): iterable
    {
        yield 'no auth attribute' => [
            'controller' => new
                class {
                    public function __invoke(): void {}
                },
            'supports' => false,
        ];

        yield 'single auth attribute' => [
            'controller' => new
                #[StorageApiTokenAuth(['foo-feature'])]
                class {
                    public function __invoke(): void {}
                },
            'supports' => true,
        ];

        yield 'multiple auth attributes' => [
            'controller' => new
                #[StorageApiTokenAuth(['foo-feature'])]
                #[ManageApiTokenAuth(['bar-feature'])]
                class {
                    public function __invoke(): void {}
                },
            'supports' => true,
        ];
    }

    #[DataProvider('provideSupportsRequestTestData')]
    public function testSupportsRequest(object $controller, bool $supports): void
    {
        $authenticators = [];

        $request = $this->createControllerRequest($controller, []);

        $authenticator = $this->createAuthenticator($controller, $authenticators);
        $this->assertSame($supports, $authenticator->supports($request));
    }

    public function testAuthenticateRequest(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, [
            'X-Auth-Token' => 'token',
        ]);

        $token = $this->createToken('user-id');

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $this->createSuccessAuthenticator($token),
            ],
        );
        $passport = $authenticator->authenticate($request);

        self::assertSame($token, $passport->getUser());
    }

    public function testAuthenticateRequestWithNoTokenHeader(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, []);

        $token = $this->createToken('user-id');

        $tokenAuthenticator = $this->createMock(TokenAuthenticatorInterface::class);
        $tokenAuthenticator->expects(self::once())
            ->method('getTokenHeader')
            ->willReturn('X-Auth-Token')
        ;

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $tokenAuthenticator,
            ],
        );

        $this->expectException(CustomUserMessageAuthenticationException::class);
        $this->expectExceptionMessage('Authentication header "X-Auth-Token" is missing');

        $authenticator->authenticate($request);
    }

    public function testAuthenticateRequestWithFailingAuthentication(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, [
            'X-Auth-Token' => 'token',
        ]);

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $this->createAuthenticatorWithFailingAuthentication('X-Auth-Token'),
            ],
        );

        $this->expectException(AuthenticationException::class);
        $this->expectExceptionMessage('Token is not valid');

        $authenticator->authenticate($request);
    }

    public function testAuthenticateRequestWithFailingAuthorization(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, [
            'X-Auth-Token' => 'token',
        ]);

        $token = $this->createToken('user-id');

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $this->createAuthenticatorWithFailingAuthorization(
                    'X-Auth-Token',
                    $token,
                ),
            ],
        );

        $this->expectException(CustomUserMessageAuthenticationException::class);
        $this->expectExceptionMessage('Token is not authorized');

        $authenticator->authenticate($request);
    }

    public function testAuthenticateRequestWithMultipleDifferentAuthenticatorsWithDifferentTokenHeader(): void
    {
        $controller = new
            #[ManageApiTokenAuth]
            #[StorageApiTokenAuth]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, [
            'X-Auth-Token' => 'token',
        ]);

        $token = $this->createToken('user-id');

        $failingAuthenticator = $this->createMock(TokenAuthenticatorInterface::class);
        $failingAuthenticator->expects(self::once())
            ->method('getTokenHeader')
            ->willReturn('X-Other-Token')
        ;

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                ManageApiTokenAuth::class => $failingAuthenticator,
                StorageApiTokenAuth::class => $this->createSuccessAuthenticator($token),
            ],
        );
        $passport = $authenticator->authenticate($request);

        self::assertSame($token, $passport->getUser());
    }

    public function testAuthenticateRequestWithMultipleDifferentAuthenticatorsWithFailingAuthentication(): void
    {
        $controller = new
            #[ManageApiTokenAuth]
            #[StorageApiTokenAuth]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, [
            'X-Other-Token' => 'other-token',
            'X-Auth-Token' => 'token',
        ]);

        $token = $this->createToken('user-id');

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                ManageApiTokenAuth::class => $this->createAuthenticatorWithFailingAuthentication('X-Other-Token'),
                StorageApiTokenAuth::class => $this->createSuccessAuthenticator($token),
            ],
        );
        $passport = $authenticator->authenticate($request);

        self::assertSame($token, $passport->getUser());
    }

    public function testAuthenticateRequestWithMultipleDifferentAuthenticatorsWithFailingAuthorization(): void
    {
        $controller = new
            #[ManageApiTokenAuth]
            #[StorageApiTokenAuth]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, [
            'X-Other-Token' => 'other-token',
            'X-Auth-Token' => 'token',
        ]);

        $otherToken = $this->createToken('other-user-id');
        $token = $this->createToken('user-id');

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                ManageApiTokenAuth::class => $this->createAuthenticatorWithFailingAuthorization(
                    'X-Other-Token',
                    $otherToken,
                ),
                StorageApiTokenAuth::class => $this->createSuccessAuthenticator($token),
            ],
        );
        $passport = $authenticator->authenticate($request);

        self::assertSame($token, $passport->getUser());
    }

    private function createAuthenticator(object $controller, array $authenticators): AttributeAuthenticator
    {
        $controllersContainer = new Container();
        $controllersContainer->set(get_class($controller), $controller);

        $authenticatorsContainer = new Container();
        foreach ($authenticators as $attribute => $authenticator) {
            self::assertIsObject($authenticator);
            $authenticatorsContainer->set($attribute, $authenticator);
        }

        return new AttributeAuthenticator(
            new ControllerReflector($controllersContainer),
            $authenticatorsContainer,
        );
    }

    private function createControllerRequest(object $controller, array $authHeaders): Request
    {
        $request = new Request();
        $request->attributes->set('_controller', get_class($controller));
        $request->headers->replace($authHeaders);

        return $request;
    }

    private function createToken(string $userIdentifier): TokenInterface
    {
        $token = $this->createMock(TokenInterface::class);
        $token->method('getUserIdentifier')->willReturn($userIdentifier);

        return $token;
    }

    /**
     * @return TokenAuthenticatorInterface<TokenInterface>
     */
    private function createSuccessAuthenticator(TokenInterface $token): TokenAuthenticatorInterface
    {
        $authenticator = $this->createMock(TokenAuthenticatorInterface::class);
        $authenticator->expects(self::once())
            ->method('getTokenHeader')
            ->willReturn('X-Auth-Token')
        ;

        $authenticator->expects(self::once())
            ->method('authenticateToken')
            ->with(
                $this->isInstanceOf(StorageApiTokenAuth::class),
                'token',
            )
            ->willReturn($token)
        ;

        $authenticator->expects(self::once())
            ->method('authorizeToken')
            ->with(
                $this->isInstanceOf(StorageApiTokenAuth::class),
                $token,
            )
        ;

        return $authenticator;
    }

    /**
     * @return TokenAuthenticatorInterface<TokenInterface>
     */
    private function createAuthenticatorWithFailingAuthentication(string $tokenHeader): TokenAuthenticatorInterface
    {
        $authenticator = $this->createMock(TokenAuthenticatorInterface::class);
        $authenticator->expects(self::once())
            ->method('getTokenHeader')
            ->willReturn($tokenHeader)
        ;
        $authenticator->expects(self::once())
            ->method('authenticateToken')
            ->willThrowException(new AuthenticationException('Token is not valid'))
        ;
        $authenticator->expects(self::never())->method('authorizeToken');

        return $authenticator;
    }

    /**
     * @return TokenAuthenticatorInterface<TokenInterface>
     */
    private function createAuthenticatorWithFailingAuthorization(
        string $tokenHeader,
        TokenInterface $authenticatedToken,
    ): TokenAuthenticatorInterface {
        $authenticator = $this->createMock(TokenAuthenticatorInterface::class);
        $authenticator->expects(self::once())
            ->method('getTokenHeader')
            ->willReturn($tokenHeader)
        ;
        $authenticator->expects(self::once())
            ->method('authenticateToken')
            ->willReturn($authenticatedToken)
        ;
        $authenticator->expects(self::once())
            ->method('authorizeToken')
            ->willThrowException(new AccessDeniedException('Token is not authorized'))
        ;

        return $authenticator;
    }

    public function testAuthenticateRequestWithBearerToken(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, [
            'Authorization' => 'Bearer my-oauth-token',
        ]);

        $token = $this->createToken('user-id');

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $this->createMultiHeaderSuccessAuthenticator(
                    $token,
                    'my-oauth-token',
                ),
            ],
        );
        $passport = $authenticator->authenticate($request);

        self::assertSame($token, $passport->getUser());
    }

    public function testAuthenticateRequestWithXStorageApiTokenFallback(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, [
            'X-StorageApi-Token' => 'my-storage-token',
        ]);

        $token = $this->createToken('user-id');

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $this->createMultiHeaderSuccessAuthenticator(
                    $token,
                    'my-storage-token',
                ),
            ],
        );
        $passport = $authenticator->authenticate($request);

        self::assertSame($token, $passport->getUser());
    }

    public function testAuthenticateRequestBearerTokenTakesPrecedence(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        // Both headers present - Bearer should take precedence
        $request = $this->createControllerRequest($controller, [
            'Authorization' => 'Bearer my-oauth-token',
            'X-StorageApi-Token' => 'my-storage-token',
        ]);

        $token = $this->createToken('user-id');

        // The authenticator should receive the Bearer token, not the X-StorageApi-Token
        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $this->createMultiHeaderSuccessAuthenticator(
                    $token,
                    'my-oauth-token', // Bearer token value (without "Bearer " prefix)
                ),
            ],
        );
        $passport = $authenticator->authenticate($request);

        self::assertSame($token, $passport->getUser());
    }

    public function testAuthenticateRequestWithMultiHeaderAuthenticatorNoToken(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        $request = $this->createControllerRequest($controller, []);

        $multiHeaderAuthenticator = $this->createMock(MultiHeaderTokenAuthenticatorInterface::class);
        $multiHeaderAuthenticator->expects(self::once())
            ->method('getTokenHeaders')
            ->willReturn(['Authorization', 'X-StorageApi-Token'])
        ;

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $multiHeaderAuthenticator,
            ],
        );

        $this->expectException(CustomUserMessageAuthenticationException::class);
        $this->expectExceptionMessage('Authentication header "Authorization" or "X-StorageApi-Token" is missing');

        $authenticator->authenticate($request);
    }

    public function testAuthenticateRequestWithAuthorizationHeaderWithoutBearerPrefix(): void
    {
        $controller = new
            #[StorageApiTokenAuth(['foo-feature'])]
            class {
                public function __invoke(): void {}
            };

        // Authorization header without "Bearer " prefix should be used as-is
        $request = $this->createControllerRequest($controller, [
            'Authorization' => 'Basic some-basic-auth',
        ]);

        $token = $this->createToken('user-id');

        $authenticator = $this->createAuthenticator(
            $controller,
            [
                StorageApiTokenAuth::class => $this->createMultiHeaderSuccessAuthenticator(
                    $token,
                    'Basic some-basic-auth', // Full value since it's not Bearer
                ),
            ],
        );
        $passport = $authenticator->authenticate($request);

        self::assertSame($token, $passport->getUser());
    }

    /**
     * @return MultiHeaderTokenAuthenticatorInterface<TokenInterface>
     */
    private function createMultiHeaderSuccessAuthenticator(
        TokenInterface $token,
        string $expectedTokenValue,
    ): MultiHeaderTokenAuthenticatorInterface {
        $authenticator = $this->createMock(MultiHeaderTokenAuthenticatorInterface::class);
        $authenticator->expects(self::once())
            ->method('getTokenHeaders')
            ->willReturn(['Authorization', 'X-StorageApi-Token'])
        ;

        $authenticator->expects(self::once())
            ->method('authenticateToken')
            ->with(
                $this->isInstanceOf(StorageApiTokenAuth::class),
                $expectedTokenValue,
            )
            ->willReturn($token)
        ;

        $authenticator->expects(self::once())
            ->method('authorizeToken')
            ->with(
                $this->isInstanceOf(StorageApiTokenAuth::class),
                $token,
            )
        ;

        return $authenticator;
    }
}
