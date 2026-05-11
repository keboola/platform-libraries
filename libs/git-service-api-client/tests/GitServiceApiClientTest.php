<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use Keboola\GitServiceApiClient\ApiClientConfiguration;
use Keboola\GitServiceApiClient\GitServiceApiClient;
use Keboola\GitServiceApiClient\KeyPermission;
use PHPUnit\Framework\TestCase;

class GitServiceApiClientTest extends TestCase
{
    private function buildClient(MockHandler $mock): GitServiceApiClient
    {
        $stack = HandlerStack::create($mock);
        return new GitServiceApiClient(
            'https://example.test',
            'token',
            new ApiClientConfiguration(requestHandler: $stack),
        );
    }

    public function testCreateRepository(): void
    {
        $mock = new MockHandler([new Response(201, [], (string) json_encode([
            'name' => 'app-1',
            'createdAt' => '2026-04-28T10:00:00Z',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git/app-1',
        ]))]);
        $client = $this->buildClient($mock);

        $repo = $client->createRepository('app-1');

        self::assertSame('app-1', $repo->name);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://example.test/repos', (string) $request->getUri());
        self::assertSame('{"name":"app-1"}', (string) $request->getBody());
    }

    public function testGetRepository(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'name' => 'app-1',
            'createdAt' => 't',
            'defaultBranch' => 'main',
            'sshUrl' => 's',
        ]))]);
        $client = $this->buildClient($mock);

        $repo = $client->getRepository('app-1');

        self::assertSame('app-1', $repo->name);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('GET', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1', (string) $request->getUri());
    }

    public function testGetRepositoryEncodesAppId(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'name' => 'app/1', 'createdAt' => 't', 'defaultBranch' => 'main', 'sshUrl' => 's',
        ]))]);
        $client = $this->buildClient($mock);

        $client->getRepository('app/1');

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('https://example.test/repos/app%2F1', (string) $request->getUri());
    }

    public function testDeleteRepository(): void
    {
        $mock = new MockHandler([new Response(204)]);
        $client = $this->buildClient($mock);

        $client->deleteRepository('app-1');

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('DELETE', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1', (string) $request->getUri());
    }

    public function testListKeys(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'keys' => [
                ['id' => 'k1', 'createdAt' => 't', 'permissions' => 'readOnly'],
                ['id' => 'k2', 'createdAt' => 't', 'permissions' => 'readWrite'],
            ],
        ]))]);
        $client = $this->buildClient($mock);

        $keys = $client->listKeys('app-1');

        self::assertCount(2, $keys);
        self::assertSame('k1', $keys[0]->id);
        self::assertSame(KeyPermission::ReadOnly, $keys[0]->permissions);
        self::assertSame(KeyPermission::ReadWrite, $keys[1]->permissions);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('https://example.test/repos/app-1/keys', (string) $request->getUri());
    }

    public function testAddKey(): void
    {
        $mock = new MockHandler([new Response(201, [], (string) json_encode([
            'id' => 'k1', 'createdAt' => 't', 'permissions' => 'readWrite',
        ]))]);
        $client = $this->buildClient($mock);

        $key = $client->addKey('app-1', 'ssh-rsa AAA', KeyPermission::ReadWrite);

        self::assertSame('k1', $key->id);
        self::assertSame(KeyPermission::ReadWrite, $key->permissions);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1/keys', (string) $request->getUri());
        self::assertSame(
            '{"publicKey":"ssh-rsa AAA","permissions":"readWrite"}',
            (string) $request->getBody(),
        );
    }

    public function testDeleteKey(): void
    {
        $mock = new MockHandler([new Response(204)]);
        $client = $this->buildClient($mock);

        $client->deleteKey('app-1', 'key-9');

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('DELETE', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1/keys/key-9', (string) $request->getUri());
    }

    public function testGetKey(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'id' => 'k1', 'createdAt' => 't', 'permissions' => 'readOnly',
        ]))]);
        $client = $this->buildClient($mock);

        $key = $client->getKey('app-1', 'k1');

        self::assertSame('k1', $key->id);
        self::assertSame(KeyPermission::ReadOnly, $key->permissions);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('GET', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1/keys/k1', (string) $request->getUri());
    }
}
