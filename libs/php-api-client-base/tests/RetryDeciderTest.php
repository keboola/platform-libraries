<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ApiClientBase\RetryDecider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\NullLogger;

class RetryDeciderTest extends TestCase
{
    public function testRetriesOn5xx(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(500)));
    }

    public function testDoesNotRetryOn4xxByDefault(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(404)));
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(429)));
    }

    public function testRetriesOnConfiguredStatusCode(): void
    {
        $decider = new RetryDecider(3, new NullLogger(), [429]);
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(429)));
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(404)));
    }

    public function testRetriesOnTransportError(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        $error = new ConnectException('connection refused', new Request('GET', '/'));
        self::assertTrue($decider(0, new Request('GET', '/'), null, $error));
    }

    public function testStopsAfterMaxRetries(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        self::assertFalse($decider(3, new Request('GET', '/'), new Response(500)));
    }

    public function testDoesNotRetryOn2xx(): void
    {
        $decider = new RetryDecider(3, new NullLogger());
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(200)));
    }

    public function testRetryableResponseCallbackDecidesForFailedResponses(): void
    {
        $decider = new RetryDecider(
            3,
            new NullLogger(),
            retryableResponse: static fn(ResponseInterface $response): ?bool => match ($response->getStatusCode()) {
                503 => false,
                409 => true,
                default => null,
            },
        );
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(503)));
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(409)));
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(500)));
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(404)));
    }

    public function testRetryableResponseWinsOverRetryableStatusCodes(): void
    {
        $decider = new RetryDecider(3, new NullLogger(), [429], static fn(ResponseInterface $response): bool => false);
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(429)));
    }

    public function testNullRetryableResponseFallsBackToRetryableStatusCodes(): void
    {
        $decider = new RetryDecider(3, new NullLogger(), [429], static fn(ResponseInterface $response): ?bool => null);
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(429)));
    }

    public function testRetryableResponseTrueShortcutRetriesEveryFailedResponse(): void
    {
        $decider = new RetryDecider(3, new NullLogger(), retryableResponse: true);
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(404)));
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(503)));
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(200)));
        self::assertFalse($decider(3, new Request('GET', '/'), new Response(503)));
    }

    public function testRetryableResponseFalseShortcutRetriesNoFailedResponse(): void
    {
        $decider = new RetryDecider(3, new NullLogger(), [429], retryableResponse: false);
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(503)));
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(429)));
        $error = new ConnectException('connection refused', new Request('GET', '/'));
        self::assertTrue($decider(0, new Request('GET', '/'), null, $error));
    }

    public function testRetryableResponseCallbackIsNotAskedForSuccessOrTransportError(): void
    {
        $decider = new RetryDecider(
            3,
            new NullLogger(),
            retryableResponse: static function (ResponseInterface $response): ?bool {
                self::fail('Callback must only be asked for a failed response.');
            },
        );
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(200)));
        $error = new ConnectException('connection refused', new Request('GET', '/'));
        self::assertTrue($decider(0, new Request('GET', '/'), null, $error));
    }

    public function testResponseBodyIsRewoundAfterRetryableResponseCallback(): void
    {
        $response = new Response(503, [], '{"context":{"errorCode":"x"}}');
        $decider = new RetryDecider(
            3,
            new NullLogger(),
            retryableResponse: static fn(ResponseInterface $response): ?bool
                => $response->getBody()->getContents() === '' ? false : null,
        );

        self::assertTrue($decider(0, new Request('GET', '/'), $response));
        self::assertSame('{"context":{"errorCode":"x"}}', $response->getBody()->getContents());
    }
}
