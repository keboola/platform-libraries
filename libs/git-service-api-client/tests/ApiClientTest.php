<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\GitServiceApiClient\ApiClient;
use Keboola\GitServiceApiClient\ApiClientConfiguration;
use Keboola\GitServiceApiClient\Exception\ClientException;
use Keboola\GitServiceApiClient\Model\Repository;
use PHPUnit\Framework\TestCase;

class ApiClientTest extends TestCase
{
    public function testAddsAuthHeader(): void
    {
        $mock = new MockHandler([new Response(200, [], '{}')]);
        $stack = HandlerStack::create($mock);

        $client = new ApiClient(
            'https://example.test',
            'secret-token',
            new ApiClientConfiguration(requestHandler: $stack),
        );
        $client->sendRequest(new Request('GET', 'foo'));

        $lastRequest = $mock->getLastRequest();
        self::assertNotNull($lastRequest);
        self::assertSame('secret-token', $lastRequest->getHeader('X-KBC-ManageApiToken')[0]);
    }

    public function testRetriesOn5xx(): void
    {
        $mock = new MockHandler([
            new Response(500),
            new Response(500),
            new Response(200, [], '{}'),
        ]);
        $stack = HandlerStack::create($mock);

        $client = new ApiClient(
            'https://example.test',
            'token',
            new ApiClientConfiguration(backoffMaxTries: 3, requestHandler: $stack),
        );
        $client->sendRequest(new Request('GET', 'foo'));

        // If retries didn't fire, MockHandler would still hold remaining responses.
        self::assertSame(0, $mock->count());
    }

    public function testThrowsClientExceptionOn4xxWithErrorCodeBody(): void
    {
        $mock = new MockHandler([
            new Response(404, [], '{"code":"repository.notFound","error":"repo missing"}'),
        ]);
        $stack = HandlerStack::create($mock);

        $client = new ApiClient(
            'https://example.test',
            'token',
            new ApiClientConfiguration(requestHandler: $stack),
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('repository.notFound: repo missing');
        $this->expectExceptionCode(404);
        $client->sendRequest(new Request('GET', 'foo'));
    }

    public function testThrowsClientExceptionOn4xxWithoutJson(): void
    {
        $mock = new MockHandler([new Response(400, [], 'plain text error')]);
        $stack = HandlerStack::create($mock);

        $client = new ApiClient(
            'https://example.test',
            'token',
            new ApiClientConfiguration(requestHandler: $stack),
        );

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(400);
        $client->sendRequest(new Request('GET', 'foo'));
    }

    public function testMapsResponseIntoSingleModel(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'name' => 'app-1',
            'createdAt' => 'now',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git/app-1',
            'httpsUrl' => 'https://git/app-1.git',
        ]))]);
        $stack = HandlerStack::create($mock);

        $client = new ApiClient(
            'https://example.test',
            'token',
            new ApiClientConfiguration(requestHandler: $stack),
        );

        $repo = $client->sendRequestAndMapResponse(
            new Request('GET', 'repos/app-1'),
            Repository::class,
        );

        self::assertSame('app-1', $repo->name);
    }

    public function testMapsResponseIntoListOfModels(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            ['name' => 'a1', 'createdAt' => 't', 'defaultBranch' => 'main', 'sshUrl' => 's1', 'httpsUrl' => 'h1'],
            ['name' => 'a2', 'createdAt' => 't', 'defaultBranch' => 'main', 'sshUrl' => 's2', 'httpsUrl' => 'h2'],
        ]))]);
        $stack = HandlerStack::create($mock);

        $client = new ApiClient(
            'https://example.test',
            'token',
            new ApiClientConfiguration(requestHandler: $stack),
        );

        $repos = $client->sendRequestAndMapResponse(
            new Request('GET', 'repos'),
            Repository::class,
            isList: true,
        );

        self::assertCount(2, $repos);
        self::assertSame('a1', $repos[0]->name);
    }

    public function testThrowsClientExceptionOnInvalidJson(): void
    {
        $mock = new MockHandler([new Response(200, [], 'not json')]);
        $stack = HandlerStack::create($mock);

        $client = new ApiClient(
            'https://example.test',
            'token',
            new ApiClientConfiguration(requestHandler: $stack),
        );

        $this->expectException(ClientException::class);
        $client->sendRequestAndMapResponse(
            new Request('GET', 'foo'),
            Repository::class,
        );
    }
}
