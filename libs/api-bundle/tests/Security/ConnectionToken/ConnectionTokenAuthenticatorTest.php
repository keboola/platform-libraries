<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\ConnectionToken;

use Generator;
use Keboola\ApiBundle\Attribute\ConnectionTokenAuth;
use Keboola\ApiBundle\Security\ConnectionToken\ConnectionTokenAuthenticator;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\ApiBundle\Security\StorageApiToken\StorageApiTokenExchange;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\Security\Core\Exception\AccessDeniedException;

class ConnectionTokenAuthenticatorTest extends TestCase
{
    // -------------------------------------------------------------------------
    // extractToken
    // -------------------------------------------------------------------------

    /**
     * @param array<string, string> $headers
     */
    #[DataProvider('provideExtractTokenData')]
    public function testExtractToken(
        string $testName,
        array $headers,
        ?string $expectedToken,
    ): void {
        $authenticator = new ConnectionTokenAuthenticator(
            $this->createMock(StorageApiTokenExchange::class),
        );

        $request = Request::create('https://keboola.com');
        foreach ($headers as $name => $value) {
            $request->headers->set($name, $value);
        }

        self::assertSame($expectedToken, $authenticator->extractToken($request), $testName);
    }

    public static function provideExtractTokenData(): Generator
    {
        yield 'bearer kbc_at_ token' => [
            'testName' => 'Bearer kbc_at_abc returns kbc_at_abc',
            'headers' => ['Authorization' => 'Bearer kbc_at_abc'],
            'expectedToken' => 'kbc_at_abc',
        ];

        yield 'bearer kbc_pat_ token' => [
            'testName' => 'Bearer kbc_pat_abc returns kbc_pat_abc',
            'headers' => ['Authorization' => 'Bearer kbc_pat_abc'],
            'expectedToken' => 'kbc_pat_abc',
        ];

        yield 'raw kbc_at_ without bearer scheme' => [
            'testName' => 'Authorization: kbc_at_abc without Bearer returns kbc_at_abc',
            'headers' => ['Authorization' => 'kbc_at_abc'],
            'expectedToken' => 'kbc_at_abc',
        ];

        yield 'bearer legacy token' => [
            'testName' => 'Bearer legacy-token is not programmatic, returns null',
            'headers' => ['Authorization' => 'Bearer legacy-token'],
            'expectedToken' => null,
        ];

        yield 'only x-storageapi-token header' => [
            'testName' => 'Only X-StorageApi-Token header without Authorization returns null',
            'headers' => ['X-StorageApi-Token' => 'kbc_at_abc'],
            'expectedToken' => null,
        ];

        yield 'no headers at all' => [
            'testName' => 'No headers returns null',
            'headers' => [],
            'expectedToken' => null,
        ];
    }

    // -------------------------------------------------------------------------
    // authenticateToken
    // -------------------------------------------------------------------------

    public function testAuthenticateTokenDelegatesToExchangeWithDefaultProjectIdHeader(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-KBC-ProjectId', '42');

        $tokenExchange = $this->createMock(StorageApiTokenExchange::class);
        $tokenExchange
            ->expects(self::once())
            ->method('exchange')
            ->with($request, 'kbc_at_abc', 'X-KBC-ProjectId')
            ->willReturn($expectedToken);

        $authenticator = new ConnectionTokenAuthenticator($tokenExchange);

        $result = $authenticator->authenticateToken(new ConnectionTokenAuth(), 'kbc_at_abc', $request);

        self::assertSame($expectedToken, $result);
    }

    public function testAuthenticateTokenForwardsCustomProjectIdHeader(): void
    {
        $expectedToken = $this->createMock(StorageApiToken::class);

        $request = Request::create('https://keboola.com');
        $request->headers->set('X-Custom-Project', '99');

        $tokenExchange = $this->createMock(StorageApiTokenExchange::class);
        $tokenExchange
            ->expects(self::once())
            ->method('exchange')
            ->with($request, 'kbc_at_xyz', 'X-Custom-Project')
            ->willReturn($expectedToken);

        $authenticator = new ConnectionTokenAuthenticator($tokenExchange, 'X-Custom-Project');

        $result = $authenticator->authenticateToken(new ConnectionTokenAuth(), 'kbc_at_xyz', $request);

        self::assertSame($expectedToken, $result);
    }

    // -------------------------------------------------------------------------
    // authorizeToken
    // -------------------------------------------------------------------------

    public function testAuthorizeTokenSucceedsWhenAllRequiredFeaturesPresent(): void
    {
        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::atLeastOnce())
            ->method('getFeatures')
            ->willReturn(['a', 'b', 'c']);

        $authenticator = new ConnectionTokenAuthenticator(
            $this->createMock(StorageApiTokenExchange::class),
        );

        // No exception expected; the getFeatures() mock expectation is the assertion.
        $authenticator->authorizeToken(new ConnectionTokenAuth(['a']), $storageApiToken);
    }

    public function testAuthorizeTokenSucceedsWhenNoFeaturesRequired(): void
    {
        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::once())
            ->method('getFeatures')
            ->willReturn([]);

        $authenticator = new ConnectionTokenAuthenticator(
            $this->createMock(StorageApiTokenExchange::class),
        );

        // No exception expected; the getFeatures() mock expectation is the assertion.
        $authenticator->authorizeToken(new ConnectionTokenAuth([]), $storageApiToken);
    }

    public function testAuthorizeTokenThrowsWhenRequiredFeatureIsMissing(): void
    {
        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::once())
            ->method('getFeatures')
            ->willReturn(['a', 'b']);

        $authenticator = new ConnectionTokenAuthenticator(
            $this->createMock(StorageApiTokenExchange::class),
        );

        $this->expectException(AccessDeniedException::class);
        $this->expectExceptionMessage('missing following features');

        $authenticator->authorizeToken(new ConnectionTokenAuth(['c']), $storageApiToken);
    }

    public function testAuthorizeTokenExceptionMessageContainsMissingFeatureName(): void
    {
        $storageApiToken = $this->createMock(StorageApiToken::class);
        $storageApiToken
            ->expects(self::once())
            ->method('getFeatures')
            ->willReturn(['a']);

        $authenticator = new ConnectionTokenAuthenticator(
            $this->createMock(StorageApiTokenExchange::class),
        );

        $this->expectException(AccessDeniedException::class);
        $this->expectExceptionMessage('missing-feature');

        $authenticator->authorizeToken(new ConnectionTokenAuth(['missing-feature']), $storageApiToken);
    }
}
