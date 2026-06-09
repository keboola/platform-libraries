<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ApiClientBase\RetryDecider;
use PHPUnit\Framework\TestCase;
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
}
