<?php

declare(strict_types=1);

namespace Keboola\EncryptionApiClient\Tests;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\EncryptionApiClient\Exception\ClientException;
use Keboola\EncryptionApiClient\Migrations;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ResponseInterface;

class MigrationsTest extends TestCase
{
    public function testMigrateConfiguration(): void
    {
        $mock = new MockHandler(
            [
                new Response(
                    200,
                    ['Content-Type' => 'application/json'],
                    '{"message":"Configuration with ID \'1234\' successfully migrated to stack \'some-stack\'."'
                    .',"data":{"destinationStack":"some-stack","componentId":"sandboxes.data-apps","configId":"1234"'
                    . ',"branchId":"102"}}',
                ),
            ],
        );

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $migrations = new Migrations(
            'some-token',
            ['handler' => $stack, 'url' => 'https://encryption.keboola.com'],
        );
        $result = $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
        );
        self::assertIsArray($result);
        self::assertSame(
            [
                'message' => 'Configuration with ID \'1234\' successfully migrated to stack \'some-stack\'.',
                'data' => [
                    'destinationStack' => 'some-stack',
                    'componentId' => 'sandboxes.data-apps',
                    'configId' => '1234',
                    'branchId' => '102',
                ],
            ],
            $result,
        );

        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertSame(
            'https://encryption.keboola.com/migrate-configuration',
            (string) $request->getUri(),
        );
        self::assertSame('POST', $request->getMethod());
        self::assertSame('some-token', $request->getHeader('X-KBC-ManageApiToken')[0]);
    }

    public function testRetryCurlExceptionFail(): void
    {
        $mock = new MockHandler(
            [
                new Response(500, ['Content-Type' => 'application/json'], 'not used'),
                new Response(500, ['Content-Type' => 'application/json'], 'not used'),
                new Response(500, ['Content-Type' => 'application/json'], 'not used'),
            ],
            function (ResponseInterface $a) {
                // abusing the mockhandler here: override the mock response and throw a Request exception
                throw new RequestException(
                    'Encryption API error: cURL error 56: OpenSSL SSL_read: Connection reset by peer, errno 104',
                    new Request('GET', 'https://example.com'),
                    null,
                    null,
                    [
                        'errno' => 56,
                        'error' => 'OpenSSL SSL_read: Connection reset by peer, errno 104',
                    ],
                );
            },
        );

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $migrations = new Migrations(
            'some-token',
            ['handler' => $stack, 'url' => 'https://encryption.keboola.com', 'backoffMaxTries' => 2],
        );
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Encryption API error: Encryption API error: cURL error 56:');
        $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
        );
    }

    public function testRetryCurlException(): void
    {
        $mock = new MockHandler(
            [
                new Response(500, ['Content-Type' => 'application/json'], 'not used'),
                new Response(500, ['Content-Type' => 'application/json'], 'not used'),
                new Response(
                    200,
                    ['Content-Type' => 'application/json'],
                    '{"message":"Configuration with ID \'1234\' successfully migrated to stack \'some-stack\'."'
                    .',"data":{"destinationStack":"some-stack","componentId":"sandboxes.data-apps","configId":"1234"'
                    . ',"branchId":"102"}}',
                ),
            ],
            function (ResponseInterface $a) {
                if ($a->getStatusCode() === 500) {
                    // abusing the mockhandler here: override the mock response and throw a Request exception
                    throw new RequestException(
                        'Encryption API error: cURL error 56: OpenSSL SSL_read: Connection reset by peer, errno 104',
                        new Request('GET', 'https://example.com'),
                        null,
                        null,
                        [
                            'errno' => 56,
                            'error' => 'OpenSSL SSL_read: Connection reset by peer, errno 104',
                        ],
                    );
                }
            },
        );

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $migrations = new Migrations(
            'some-token',
            ['handler' => $stack, 'url' => 'https://encryption.keboola.com'],
        );
        $result = $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
        );
        self::assertIsArray($result);
        self::assertSame(
            [
                'message' => 'Configuration with ID \'1234\' successfully migrated to stack \'some-stack\'.',
                'data' => [
                    'destinationStack' => 'some-stack',
                    'componentId' => 'sandboxes.data-apps',
                    'configId' => '1234',
                    'branchId' => '102',
                ],
            ],
            $result,
        );
    }

    public function testRetryCurlExceptionWithoutContext(): void
    {
        $mock = new MockHandler(
            [
                new Response(500, ['Content-Type' => 'application/json'], 'not used'),
            ],
            function (ResponseInterface $a) {
                // abusing the mockhandler here: override the mock response and throw a Request exception
                throw new RequestException(
                    'Encryption API error: cURL error 56: OpenSSL SSL_read: Connection reset by peer, errno 104',
                    new Request('GET', 'https://example.com'),
                    null,
                    null,
                    [],
                );
            },
        );

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $migrations = new Migrations(
            'some-token',
            ['handler' => $stack, 'url' => 'https://encryption.keboola.com'],
        );
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Encryption API error: Encryption API error: cURL error 56:');
        $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
        );
    }
}
