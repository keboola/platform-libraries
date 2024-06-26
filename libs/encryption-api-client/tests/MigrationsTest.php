<?php

declare(strict_types=1);

namespace Keboola\EncryptionApiClient\Tests;

use GuzzleHttp\Exception\ConnectException;
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
                    .',"data":{"destinationStack":"some-stack","componentId":"keboola.some-component",'
                    .'"configId":"1234","branchId":"102"}}',
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
                    'componentId' => 'keboola.some-component',
                    'configId' => '1234',
                    'branchId' => '102',
                ],
            ],
            $result,
        );

        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertSame('POST', $request->getMethod());
        self::assertSame('application/json', $request->getHeader('Accept')[0]);
        self::assertSame('application/json', $request->getHeader('Content-Type')[0]);
        self::assertSame('some-token', $request->getHeader('X-KBC-ManageApiToken')[0]);
        self::assertSame(
            'https://encryption.keboola.com/migrate-configuration',
            (string) $request->getUri(),
        );
    }

    public function testDryRun(): void
    {
        $mock = new MockHandler([
            new Response(),
            new Response(),
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $migrations = new Migrations(
            'some-token',
            ['handler' => $stack, 'url' => 'https://encryption.keboola.com'],
        );

        $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
            false
        );

        $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
            true
        );

        /** @var Request $request */

        $request = array_shift($container)['request'];
        self::assertSame(
            'https://encryption.keboola.com/migrate-configuration',
            (string) $request->getUri(),
        );

        $request = array_shift($container)['request'];
        self::assertSame(
            'https://encryption.keboola.com/migrate-configuration?dry-run=true',
            (string) $request->getUri(),
        );
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

    public function testInvalidJsonResponse(): void
    {
        $mock = new MockHandler(
            [
                new Response(
                    200,
                    ['Content-Type' => 'application/json'],
                    '{"message":"Configuration with ID \'1234\' successf#$@&*',
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
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Unable to parse response body into JSON: Control character error, possibly incorrectly encoded'
        );
        $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
        );
    }

    public function testConnectionException(): void
    {
        $mock = new MockHandler(
            [
                new ConnectException('connection error', new Request('GET', 'test')),
            ],
        );

        $stack = HandlerStack::create($mock);

        $migrations = new Migrations(
            'some-token',
            ['handler' => $stack, 'url' => 'https://encryption.keboola.com'],
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Encryption API error: connection error'
        );
        $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
        );
    }

    public function testRequestException(): void
    {
        $mock = new MockHandler(
            [
                new RequestException(
                    'simple error message',
                    new Request('GET', 'test')
                ),
                new RequestException(
                    'error message to be overridden with response body message',
                    new Request('GET', 'test'),
                    new Response(400, [], '{"message":"bad bad request"}')
                ),
            ],
        );

        $stack = HandlerStack::create($mock);

        $migrations = new Migrations(
            'some-token',
            ['handler' => $stack, 'url' => 'https://encryption.keboola.com'],
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Encryption API error: simple error message'
        );
        $migrations->migrateConfiguration(
            'some-token',
            'some-stack',
            'some-token',
            'keboola.some-component',
            '1234',
            '102',
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Encryption API error: bad bad request'
        );
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
