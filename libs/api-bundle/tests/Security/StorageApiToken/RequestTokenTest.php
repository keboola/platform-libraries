<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Generator;
use Keboola\ApiBundle\Security\StorageApiToken\RequestToken;
use Keboola\ApiBundle\Security\StorageApiToken\RequestTokenType;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;

class RequestTokenTest extends TestCase
{
    private const PROGRAMMATIC_TOKEN = 'kbc_at_secret';

    /**
     * @param array<string, string> $headers
     */
    #[DataProvider('provideClassifications')]
    public function testTryFromRequestClassifiesRequest(
        array $headers,
        string $expectedToken,
        RequestTokenType $expectedType,
    ): void {
        $request = Request::create('https://keboola.com');
        foreach ($headers as $name => $value) {
            $request->headers->set($name, $value);
        }

        $credential = RequestToken::tryFromRequest($request);

        self::assertNotNull($credential);
        self::assertSame($expectedToken, $credential->token);
        self::assertSame($expectedType, $credential->type);
    }

    public static function provideClassifications(): Generator
    {
        yield 'legacy X-StorageApi-Token' => [
            'headers' => ['X-StorageApi-Token' => 'my-token'],
            'expectedToken' => 'my-token',
            'expectedType' => RequestTokenType::StorageToken,
        ];
        yield 'OAuth Authorization: Bearer' => [
            'headers' => ['Authorization' => 'Bearer my-bearer-token'],
            'expectedToken' => 'my-bearer-token',
            'expectedType' => RequestTokenType::OAuthToken,
        ];
        yield 'non-Bearer Authorization taken verbatim as legacy token' => [
            'headers' => ['Authorization' => 'some-token-without-bearer'],
            'expectedToken' => 'some-token-without-bearer',
            'expectedType' => RequestTokenType::StorageToken,
        ];
        yield 'Authorization: Bearer wins over X-StorageApi-Token' => [
            'headers' => ['X-StorageApi-Token' => 'storage-token', 'Authorization' => 'Bearer bearer-token'],
            'expectedToken' => 'bearer-token',
            'expectedType' => RequestTokenType::OAuthToken,
        ];
        yield 'programmatic access token via Bearer' => [
            'headers' => ['Authorization' => 'Bearer ' . self::PROGRAMMATIC_TOKEN],
            'expectedToken' => self::PROGRAMMATIC_TOKEN,
            'expectedType' => RequestTokenType::Programmatic,
        ];
        yield 'programmatic personal access token via Bearer' => [
            'headers' => ['Authorization' => 'Bearer kbc_pat_secret'],
            'expectedToken' => 'kbc_pat_secret',
            'expectedType' => RequestTokenType::Programmatic,
        ];
        yield 'bare programmatic prefix via Bearer is still programmatic' => [
            'headers' => ['Authorization' => 'Bearer kbc_at_'],
            'expectedToken' => 'kbc_at_',
            'expectedType' => RequestTokenType::Programmatic,
        ];
        yield 'Bearer token merely containing a programmatic prefix is OAuth' => [
            'headers' => ['Authorization' => 'Bearer x-kbc_at_abc'],
            'expectedToken' => 'x-kbc_at_abc',
            'expectedType' => RequestTokenType::OAuthToken,
        ];
        yield 'Bearer token with a near-miss prefix is OAuth' => [
            'headers' => ['Authorization' => 'Bearer kbc_a_abc'],
            'expectedToken' => 'kbc_a_abc',
            'expectedType' => RequestTokenType::OAuthToken,
        ];
        // A programmatic-looking token that does not arrive as `Authorization: Bearer` is an
        // undocumented shape and stays a legacy Storage token (never exchanged).
        yield 'programmatic token via bare Authorization is legacy' => [
            'headers' => ['Authorization' => self::PROGRAMMATIC_TOKEN],
            'expectedToken' => self::PROGRAMMATIC_TOKEN,
            'expectedType' => RequestTokenType::StorageToken,
        ];
        yield 'programmatic token via X-StorageApi-Token is legacy' => [
            'headers' => ['X-StorageApi-Token' => self::PROGRAMMATIC_TOKEN],
            'expectedToken' => self::PROGRAMMATIC_TOKEN,
            'expectedType' => RequestTokenType::StorageToken,
        ];
    }

    public function testTryFromRequestReturnsNullWhenNoTokenHeader(): void
    {
        self::assertNull(RequestToken::tryFromRequest(Request::create('https://keboola.com')));
    }

    /**
     * A present-but-empty header carries no token: it yields null (→ "Authentication token is
     * missing", no Storage API call) rather than a doomed empty-token verification, and this holds
     * for both carriers.
     *
     * @param array<string, string> $headers
     */
    #[DataProvider('provideEmptyTokenHeaders')]
    public function testTryFromRequestTreatsEmptyHeaderAsNoToken(array $headers): void
    {
        $request = Request::create('https://keboola.com');
        foreach ($headers as $name => $value) {
            $request->headers->set($name, $value);
        }

        self::assertNull(RequestToken::tryFromRequest($request));
    }

    public static function provideEmptyTokenHeaders(): Generator
    {
        yield 'empty X-StorageApi-Token' => ['headers' => ['X-StorageApi-Token' => '']];
        yield 'empty Authorization' => ['headers' => ['Authorization' => '']];
        yield 'both empty' => ['headers' => ['Authorization' => '', 'X-StorageApi-Token' => '']];
    }

    public function testTryFromRequestIgnoresEmptyAuthorizationAndFallsBackToStorageApiToken(): void
    {
        $request = Request::create('https://keboola.com');
        $request->headers->set('Authorization', '');
        $request->headers->set('X-StorageApi-Token', 'legacy-token');

        $credential = RequestToken::tryFromRequest($request);

        self::assertNotNull($credential);
        self::assertSame('legacy-token', $credential->token);
        self::assertSame(RequestTokenType::StorageToken, $credential->type);
    }
}
