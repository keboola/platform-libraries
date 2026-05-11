<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\GitServiceApiClient\RetryDecider;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class RetryDeciderTest extends TestCase
{
    public function testRetriesOn5xx(): void
    {
        $decider = new RetryDecider(maxRetries: 3, logger: new NullLogger());
        $result = $decider(0, new Request('GET', '/'), new Response(500));
        self::assertTrue($result);
    }

    public function testDoesNotRetryOn4xx(): void
    {
        $decider = new RetryDecider(maxRetries: 3, logger: new NullLogger());
        $result = $decider(0, new Request('GET', '/'), new Response(404));
        self::assertFalse($result);
    }

    public function testRetriesOnTransportError(): void
    {
        $decider = new RetryDecider(maxRetries: 3, logger: new NullLogger());
        $error = new ConnectException('connection refused', new Request('GET', '/'));
        $result = $decider(0, new Request('GET', '/'), null, $error);
        self::assertTrue($result);
    }

    public function testStopsAfterMaxRetries(): void
    {
        $decider = new RetryDecider(maxRetries: 3, logger: new NullLogger());
        $result = $decider(3, new Request('GET', '/'), new Response(500));
        self::assertFalse($result);
    }

    public function testDoesNotRetryOn2xx(): void
    {
        $decider = new RetryDecider(maxRetries: 3, logger: new NullLogger());
        $result = $decider(0, new Request('GET', '/'), new Response(200));
        self::assertFalse($result);
    }
}
