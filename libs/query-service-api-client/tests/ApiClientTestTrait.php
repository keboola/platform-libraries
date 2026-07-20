<?php

declare(strict_types=1);

namespace Keboola\QueryApi\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Throwable;

trait ApiClientTestTrait
{
    abstract public static function assertSame(mixed $expected, mixed $actual, string $message = ''): void;

    /**
     * @param list<array{request: Request, response: Response}>|null $requestsHistory
     * @param list<Response|Throwable> $responses
     * @param-out list<array{request: Request, response: Response}> $requestsHistory
     */
    private static function createRequestHandler(?array &$requestsHistory, array $responses): HandlerStack
    {
        $requestsHistory = [];

        $stack = HandlerStack::create(new MockHandler($responses));
        $stack->push(Middleware::history($requestsHistory));

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
