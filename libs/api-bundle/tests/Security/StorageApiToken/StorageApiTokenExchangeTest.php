<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Generator;
use Keboola\ApiBundle\AuthBridge\Exception\InvalidResolverRequestException;
use Keboola\ApiBundle\AuthBridge\Exception\ProjectAccessDeniedException;
use Keboola\ApiBundle\AuthBridge\Exception\ResolverUnavailableException;
use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use Keboola\ApiBundle\AuthBridge\Exception\UnauthorizedSubjectTokenException;
use Keboola\ApiBundle\AuthBridge\ResolvedStorageToken;
use Keboola\ApiBundle\AuthBridge\StorageTokenResolverInterface;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenExchange;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\CustomUserMessageAuthenticationException;

class StorageApiTokenExchangeTest extends TestCase
{
    private const PROJECT_ID_HEADER = 'X-KBC-ProjectId';

    // ---------------------------------------------------------------------------
    // Project-id header validation
    // ---------------------------------------------------------------------------

    #[DataProvider('provideInvalidProjectIdHeaders')]
    public function testExchangeThrowsWith400ForInvalidProjectIdHeader(?string $headerValue): void
    {
        $resolver = $this->createMock(StorageTokenResolverInterface::class);
        $resolver
            ->expects(self::never())
            ->method('resolve');

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromResolvedToken');

        $request = Request::create('https://keboola.com');
        if ($headerValue !== null) {
            $request->headers->set(self::PROJECT_ID_HEADER, $headerValue);
        }

        $exchange = new StorageApiTokenExchange($resolver, $tokenFactory);

        $exception = null;
        try {
            $exchange->exchange($request, 'kbc_at_secret', self::PROJECT_ID_HEADER);
        } catch (CustomUserMessageAuthenticationException $e) {
            $exception = $e;
        }

        self::assertNotNull($exception, 'Expected CustomUserMessageAuthenticationException was not thrown.');
        self::assertSame(400, $exception->getCode());
    }

    public static function provideInvalidProjectIdHeaders(): Generator
    {
        yield 'missing header' => ['headerValue' => null];
        yield 'empty string' => ['headerValue' => ''];
        yield 'non-numeric string' => ['headerValue' => 'abc'];
        yield 'zero' => ['headerValue' => '0'];
        yield 'negative number' => ['headerValue' => '-5'];
    }

    // ---------------------------------------------------------------------------
    // Resolver exception mapping
    // ---------------------------------------------------------------------------

    /**
     * @param class-string<StorageTokenResolverException> $resolverExceptionClass
     */
    #[DataProvider('provideResolverExceptionMapping')]
    public function testExchangeMapsResolverExceptionToExpectedCode(
        string $resolverExceptionClass,
        int $expectedCode,
    ): void {
        $resolver = $this->createMock(StorageTokenResolverInterface::class);
        $resolver
            ->expects(self::once())
            ->method('resolve')
            ->willThrowException(new $resolverExceptionClass('resolver error'));

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::never())
            ->method('createFromResolvedToken');

        $request = Request::create('https://keboola.com');
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $exchange = new StorageApiTokenExchange($resolver, $tokenFactory);

        $exception = null;
        try {
            $exchange->exchange($request, 'kbc_at_secret', self::PROJECT_ID_HEADER);
        } catch (CustomUserMessageAuthenticationException $e) {
            $exception = $e;
        }

        self::assertNotNull($exception, 'Expected CustomUserMessageAuthenticationException was not thrown.');
        self::assertSame($expectedCode, $exception->getCode());
    }

    public static function provideResolverExceptionMapping(): Generator
    {
        yield 'unauthorized subject token -> 401' => [
            'resolverExceptionClass' => UnauthorizedSubjectTokenException::class,
            'expectedCode' => 401,
        ];
        yield 'project access denied -> 403' => [
            'resolverExceptionClass' => ProjectAccessDeniedException::class,
            'expectedCode' => 403,
        ];
        yield 'invalid resolver request -> 400' => [
            'resolverExceptionClass' => InvalidResolverRequestException::class,
            'expectedCode' => 400,
        ];
        yield 'resolver unavailable -> 502' => [
            'resolverExceptionClass' => ResolverUnavailableException::class,
            'expectedCode' => 502,
        ];
        yield 'base resolver exception -> 502' => [
            'resolverExceptionClass' => StorageTokenResolverException::class,
            'expectedCode' => 502,
        ];
    }

    // ---------------------------------------------------------------------------
    // Success path
    // ---------------------------------------------------------------------------

    public function testExchangeSuccess(): void
    {
        $resolvedToken = new ResolvedStorageToken(
            storageToken: 'legacy-tok',
            projectId: 123,
            tokenId: '1',
            userId: '2',
            expiresAt: null,
        );

        $expectedStorageApiToken = $this->createMock(StorageApiToken::class);

        $resolver = $this->createMock(StorageTokenResolverInterface::class);
        $resolver
            ->expects(self::once())
            ->method('resolve')
            ->with(123, 'kbc_at_secret')
            ->willReturn($resolvedToken);

        $request = Request::create('https://keboola.com');
        $request->headers->set(self::PROJECT_ID_HEADER, '123');

        $tokenFactory = $this->createMock(StorageApiTokenFactory::class);
        $tokenFactory
            ->expects(self::once())
            ->method('createFromResolvedToken')
            ->with($request, 'legacy-tok')
            ->willReturn($expectedStorageApiToken);

        $exchange = new StorageApiTokenExchange($resolver, $tokenFactory);

        $result = $exchange->exchange($request, 'kbc_at_secret', self::PROJECT_ID_HEADER);

        self::assertSame($expectedStorageApiToken, $result);
    }
}
