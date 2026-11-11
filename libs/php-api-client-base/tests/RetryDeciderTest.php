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

    public function testDoesNotRetryResponseVetoedAsNonRetryable(): void
    {
        $decider = new RetryDecider(
            3,
            new NullLogger(),
            isNonRetryableResponse: static fn(ResponseInterface $response): bool => $response->getStatusCode() === 503,
        );
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(503)));
        self::assertTrue($decider(0, new Request('GET', '/'), new Response(500)));
    }

    public function testVetoWinsOverConfiguredStatusCode(): void
    {
        $decider = new RetryDecider(
            3,
            new NullLogger(),
            [429],
            static fn(ResponseInterface $response): bool => true,
        );
        self::assertFalse($decider(0, new Request('GET', '/'), new Response(429)));
    }

    public function testVetoIsNotAskedWithoutResponse(): void
    {
        $decider = new RetryDecider(
            3,
            new NullLogger(),
            isNonRetryableResponse: static function (ResponseInterface $response): bool {
                self::fail('Veto must not be called without a response.');
            },
        );
        $error = new ConnectException('connection refused', new Request('GET', '/'));
        self::assertTrue($decider(0, new Request('GET', '/'), null, $error));
    }

    public function testResponseBodyIsRewoundAfterVeto(): void
    {
        $response = new Response(503, [], '{"context":{"errorCode":"x"}}');
        $decider = new RetryDecider(
            3,
            new NullLogger(),
            isNonRetryableResponse: static fn(ResponseInterface $response): bool
                => $response->getBody()->getContents() === '',
        );

        self::assertTrue($decider(0, new Request('GET', '/'), $response));
        self::assertSame('{"context":{"errorCode":"x"}}', $response->getBody()->getContents());
    }
}
