<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;

trait ApiClientTestTrait
{
    abstract public static function assertSame(mixed $expected, mixed $actual, string $message = ''): void;

    /**
     * @param list<Response> $responses
     * @param-out list<array{request: Request, response: Response}> $requestsHistory
     */
    private static function createRequestHandler(?array &$requestsHistory, array $responses): HandlerStack
    {
        $requestsHistory = [];

        // Deliberately NOT HandlerStack::create(): that would add Guzzle's httpErrors
        // middleware inside this handler, turning 4xx/5xx responses into exceptions
        // before the base client's retry middleware sees them. Using a bare stack lets
        // the base client's own httpErrors (outer to retry) be the only one, so the retry
        // decider sees real status codes — matching production behaviour.
        $stack = new HandlerStack(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));
        /** @var list<array{request: Request, response: Response}> $requestsHistory */

        return $stack;
    }

    /**
     * @param array<string, string> $headers
     */
    private static function assertRequestEquals(
        string $method,
        string $uri,
        array $headers,
        ?string $body,
        Request $request,
    ): void {
        self::assertSame($method, $request->getMethod());
        self::assertSame($uri, $request->getUri()->__toString());

        foreach ($headers as $headerName => $headerValue) {
            self::assertSame($headerValue, $request->getHeaderLine($headerName));
        }

        self::assertSame($body ?? '', $request->getBody()->getContents());
    }
}
