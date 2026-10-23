<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests\Authentication\Authenticator;

use Generator;
use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use Keboola\AzureApiClient\Authentication\Authenticator\SharedAccessSignatureAuth;
use PHPUnit\Framework\TestCase;

class SharedAccessSignatureAuthTest extends TestCase
{
    private const KEY = 'c2hhcmVkLWFjY2Vzcy1rZXk=';

    /** @dataProvider provideRequestUris */
    public function testSignsRequestResourceUri(string $requestUri, string $expectedResource): void
    {
        $authenticator = (new SharedAccessSignatureAuth('RootManageSharedAccessKey', self::KEY))
            ->createRequestAuthenticator('ignored-resource');

        $before = time();
        $request = $authenticator(new Request('POST', $requestUri));
        $after = time();

        $token = self::parseToken($request->getHeaderLine('Authorization'));

        self::assertSame(strtolower(rawurlencode($expectedResource)), $token['sr']);
        self::assertSame('RootManageSharedAccessKey', $token['skn']);
        self::assertGreaterThanOrEqual($before + 3600, (int) $token['se']);
        self::assertLessThanOrEqual($after + 3600, (int) $token['se']);
        $expectedSignature = base64_encode(hash_hmac(
            'sha256',
            strtolower(rawurlencode($expectedResource)) . "\n" . $token['se'],
            self::KEY,
            true,
        ));
        self::assertSame($expectedSignature, rawurldecode($token['sig']));
        self::assertDoesNotMatchRegularExpression('~%[0-9a-f]?[A-F]~', $token['sig']);
    }

    public static function provideRequestUris(): Generator
    {
        yield 'plain path' => [
            'requestUri' => 'https://my-namespace.servicebus.windows.net/queue-main/messages',
            'expectedResource' => 'https://my-namespace.servicebus.windows.net/queue-main/messages',
        ];

        yield 'query and explicit port are not part of the resource' => [
            'requestUri' => 'https://my-namespace.servicebus.windows.net:443/queue-main/messages/head?timeout=10',
            'expectedResource' => 'https://my-namespace.servicebus.windows.net/queue-main/messages/head',
        ];

        yield 'resource is lowercased' => [
            'requestUri' => 'https://My-Namespace.servicebus.windows.net/Queue-Main/messages/2/LOCK',
            'expectedResource' => 'https://my-namespace.servicebus.windows.net/queue-main/messages/2/lock',
        ];
    }

    public function testCustomTokenTtl(): void
    {
        $authenticator = (new SharedAccessSignatureAuth('policy', self::KEY, 1))
            ->createRequestAuthenticator('ignored-resource');

        $before = time();
        $request = $authenticator(new Request('GET', 'https://my-namespace.servicebus.windows.net/q'));
        $after = time();

        $expiry = (int) self::parseToken($request->getHeaderLine('Authorization'))['se'];
        self::assertGreaterThanOrEqual($before + 1, $expiry);
        self::assertLessThanOrEqual($after + 1, $expiry);
    }

    public function testKeyNameIsEncoded(): void
    {
        $authenticator = (new SharedAccessSignatureAuth('send & listen', self::KEY))
            ->createRequestAuthenticator('ignored-resource');

        $request = $authenticator(new Request('GET', 'https://my-namespace.servicebus.windows.net/q'));

        self::assertStringEndsWith('&skn=send%20%26%20listen', $request->getHeaderLine('Authorization'));
    }

    public function testHeaderFormat(): void
    {
        $authenticator = (new SharedAccessSignatureAuth('policy', self::KEY))
            ->createRequestAuthenticator('ignored-resource');

        $request = $authenticator(new Request('GET', 'https://my-namespace.servicebus.windows.net/q'));

        self::assertMatchesRegularExpression(
            '~^SharedAccessSignature sr=https%3a%2f%2fmy-namespace\.servicebus\.windows\.net%2fq' .
            '&sig=[A-Za-z0-9%]+&se=\d+&skn=policy$~',
            $request->getHeaderLine('Authorization'),
        );
    }

    /** @dataProvider provideInvalidArguments */
    public function testInvalidArguments(string $keyName, string $key, int $tokenTtlSeconds): void
    {
        $this->expectException(InvalidArgumentException::class);

        // @phpstan-ignore-next-line intentionally passing invalid values
        new SharedAccessSignatureAuth($keyName, $key, $tokenTtlSeconds);
    }

    public static function provideInvalidArguments(): Generator
    {
        yield 'empty key name' => [
            'keyName' => '',
            'key' => self::KEY,
            'tokenTtlSeconds' => 60,
        ];

        yield 'empty key' => [
            'keyName' => 'policy',
            'key' => '',
            'tokenTtlSeconds' => 60,
        ];

        yield 'zero ttl' => [
            'keyName' => 'policy',
            'key' => self::KEY,
            'tokenTtlSeconds' => 0,
        ];
    }

    /**
     * @return array<string, string>
     */
    private static function parseToken(string $header): array
    {
        self::assertStringStartsWith('SharedAccessSignature ', $header);

        $token = [];
        foreach (explode('&', substr($header, strlen('SharedAccessSignature '))) as $pair) {
            [$name, $value] = explode('=', $pair, 2);
            $token[$name] = $value;
        }
        self::assertSame(['sr', 'sig', 'se', 'skn'], array_keys($token));

        return $token;
    }
}
