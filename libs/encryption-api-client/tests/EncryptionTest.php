<?php

declare(strict_types=1);

namespace Keboola\EncryptionApiClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Keboola\EncryptionApiClient\Encryption;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;

class EncryptionTest extends TestCase
{
    public function testEncryptPlainTextForConfiguration(): void
    {
        $mock = new MockHandler(
            [
                new Response(
                    200,
                    ['Content-Type' => 'application/json'],
                    '{"#value":"KBC::ConfigSecure::abcdefghijkl"}',
                ),
            ],
        );

        $stack = HandlerStack::create($mock);

        $requestsMade = [];
        $history = Middleware::history($requestsMade);
        $stack->push($history);

        $encryptionClient = new Encryption(
            'some-token',
            ['handler' => $stack, 'url' => 'https://encryption.keboola.com'],
        );
        $result = $encryptionClient->encryptPlainTextForConfiguration(
            'plainValue',
            'project-id',
            'keboola.component-id',
            'config-id',
        );
        self::assertSame('KBC::ConfigSecure::abcdefghijkl', $result);

        self::assertCount(1, $requestsMade);
        $request = $requestsMade[0]['request'];
        self::assertInstanceOf(RequestInterface::class, $request);
        self::assertSame('POST', $request->getMethod());
        self::assertSame(
            'https://encryption.keboola.com/encrypt?projectId=project-id'
                . '&componentId=keboola.component-id&configId=config-id',
            (string) $request->getUri(),
        );
        self::assertSame('{"#value":"plainValue"}', $request->getBody()->getContents());
    }
}
