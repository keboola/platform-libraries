<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientConfiguration;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use Keboola\ApiClientBase\Exception\ClientException;
use Keboola\ApiClientBase\Tests\Fixtures\DummyModel;
use PHPUnit\Framework\TestCase;

class ApiClientTest extends TestCase
{
    public function testSendsWithoutAuthHeaderWhenNoAuthenticator(): void
    {
        $mock = new MockHandler([new Response(200, [], '{}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $client->sendRequest(new Request('GET', 'foo'));

        $last = $mock->getLastRequest();
        self::assertNotNull($last);
        self::assertSame([], $last->getHeader('X-KBC-ManageApiToken'));
        self::assertStringContainsString('Keboola PHP API Client', $last->getHeaderLine('User-Agent'));
    }

    public function testAddsAuthHeaderPerRequest(): void
    {
        $mock = new MockHandler([new Response(200, [], '{}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            authenticator: new ManageApiTokenAuthenticator('secret-token'),
            requestHandler: HandlerStack::create($mock),
        ));
        $client->sendRequest(new Request('GET', 'foo'));

        $last = $mock->getLastRequest();
        self::assertNotNull($last);
        self::assertSame('secret-token', $last->getHeaderLine('X-KBC-ManageApiToken'));
    }

    public function testMapsResponseToModel(): void
    {
        $mock = new MockHandler([new Response(200, [], '{"name":"foo"}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $model = $client->sendRequestAndMapResponse(new Request('GET', 'foo'), DummyModel::class);

        self::assertInstanceOf(DummyModel::class, $model);
        self::assertSame('foo', $model->name);
    }

    public function testMapsResponseToList(): void
    {
        $mock = new MockHandler([new Response(200, [], '[{"name":"a"},{"name":"b"}]')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $models = $client->sendRequestAndMapResponse(new Request('GET', 'foo'), DummyModel::class, [], true);

        self::assertCount(2, $models);
        self::assertSame('a', $models[0]->name);
        self::assertSame('b', $models[1]->name);
    }

    public function testRetriesOn5xxThenSucceeds(): void
    {
        $mock = new MockHandler([new Response(500), new Response(200, [], '{}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $client->sendRequest(new Request('GET', 'foo'));
        self::assertSame(0, $mock->count());
    }

    public function testThrowsClientExceptionWithDefaultMessageExtraction(): void
    {
        $mock = new MockHandler([new Response(400, [], '{"error":"bad input"}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
        ));
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('bad input');
        $this->expectExceptionCode(400);
        $client->sendRequest(new Request('GET', 'foo'));
    }

    public function testUsesCustomErrorMessageResolver(): void
    {
        $mock = new MockHandler([new Response(409, [], '{"code":"CONFLICT","error":"already exists"}')]);
        $client = new ApiClient('https://example.test', new ApiClientConfiguration(
            requestHandler: HandlerStack::create($mock),
            errorMessageResolver: static function (string $body): string {
                /** @var array{code?: string, error?: string} $data */
                $data = json_decode($body, true);
                return ($data['code'] ?? '') . ': ' . ($data['error'] ?? '');
            },
        ));
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('CONFLICT: already exists');
        $client->sendRequest(new Request('GET', 'foo'));
    }
}
