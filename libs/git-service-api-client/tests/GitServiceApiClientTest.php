<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use Keboola\GitServiceApiClient\ApiClientConfiguration;
use Keboola\GitServiceApiClient\CredentialType;
use Keboola\GitServiceApiClient\GitServiceApiClient;
use Keboola\GitServiceApiClient\KeyPermission;
use Keboola\GitServiceApiClient\NewCredential;
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

    public function testGetRepositoryEncodesName(): void
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

    public function testListCredentials(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'credentials' => [
                [
                    'id' => '1',
                    'type' => 'ssh_key',
                    'username' => 'app-1-svc',
                    'publicKey' => 'ssh-ed25519 AAA',
                    'permissions' => 'readOnly',
                    'createdAt' => 't',
                ],
                [
                    'id' => '2',
                    'type' => 'http_token',
                    'username' => 'app-1-bot',
                    'permissions' => 'readWrite',
                    'createdAt' => 't',
                ],
            ],
        ]))]);
        $client = $this->buildClient($mock);

        $credentials = $client->listCredentials('app-1');

        self::assertCount(2, $credentials);
        self::assertSame('1', $credentials[0]->id);
        self::assertSame(CredentialType::SshKey, $credentials[0]->type);
        self::assertSame('ssh-ed25519 AAA', $credentials[0]->publicKey);
        self::assertSame(CredentialType::HttpToken, $credentials[1]->type);
        self::assertNull($credentials[1]->publicKey);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('GET', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1/credentials', (string) $request->getUri());
    }

    public function testGetCredential(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'id' => '42',
            'type' => 'ssh_key',
            'username' => 'app-1-svc',
            'publicKey' => 'ssh-ed25519 AAA',
            'permissions' => 'readWrite',
            'createdAt' => 't',
        ]))]);
        $client = $this->buildClient($mock);

        $credential = $client->getCredential('app-1', '42');

        self::assertSame('42', $credential->id);
        self::assertSame(CredentialType::SshKey, $credential->type);
        self::assertSame(KeyPermission::ReadWrite, $credential->permissions);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('GET', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1/credentials/42', (string) $request->getUri());
    }

    public function testGetCredentialEncodesIds(): void
    {
        $mock = new MockHandler([new Response(200, [], (string) json_encode([
            'id' => 'a/b',
            'type' => 'ssh_key',
            'username' => 'u',
            'publicKey' => 'k',
            'permissions' => 'readOnly',
            'createdAt' => 't',
        ]))]);
        $client = $this->buildClient($mock);

        $client->getCredential('app/1', 'a/b');

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame(
            'https://example.test/repos/app%2F1/credentials/a%2Fb',
            (string) $request->getUri(),
        );
    }

    public function testCreateCredentialWithSshKey(): void
    {
        $mock = new MockHandler([new Response(201, [], (string) json_encode([
            'id' => '42',
            'type' => 'ssh_key',
            'username' => 'app-1-svc',
            'publicKey' => 'ssh-ed25519 AAA',
            'permissions' => 'readWrite',
            'createdAt' => 't',
        ]))]);
        $client = $this->buildClient($mock);

        $credential = $client->createCredential(
            'app-1',
            NewCredential::sshKey('svc', 'ssh-ed25519 AAA', KeyPermission::ReadWrite),
        );

        self::assertSame('42', $credential->id);
        self::assertSame(CredentialType::SshKey, $credential->type);
        self::assertSame('ssh-ed25519 AAA', $credential->publicKey);
        self::assertNull($credential->secret);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1/credentials', (string) $request->getUri());
        self::assertSame(
            '{"type":"ssh_key","username":"svc","permissions":"readWrite","publicKey":"ssh-ed25519 AAA"}',
            (string) $request->getBody(),
        );
    }

    public function testCreateCredentialWithHttpToken(): void
    {
        $mock = new MockHandler([new Response(201, [], (string) json_encode([
            'id' => '43',
            'type' => 'http_token',
            'username' => 'app-1-bot',
            'secret' => 'ghs_abc123',
            'httpsUrl' => 'https://forgejo.example.com/keboola/app-1.git',
            'permissions' => 'readOnly',
            'createdAt' => 't',
        ]))]);
        $client = $this->buildClient($mock);

        $credential = $client->createCredential(
            'app-1',
            NewCredential::httpToken('bot', KeyPermission::ReadOnly),
        );

        self::assertSame('43', $credential->id);
        self::assertSame(CredentialType::HttpToken, $credential->type);
        self::assertSame('ghs_abc123', $credential->secret);
        self::assertSame('https://forgejo.example.com/keboola/app-1.git', $credential->httpsUrl);
        self::assertNull($credential->publicKey);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('POST', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1/credentials', (string) $request->getUri());
        self::assertSame(
            '{"type":"http_token","username":"bot","permissions":"readOnly"}',
            (string) $request->getBody(),
        );
    }

    public function testDeleteCredential(): void
    {
        $mock = new MockHandler([new Response(204)]);
        $client = $this->buildClient($mock);

        $client->deleteCredential('app-1', '42');

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        self::assertSame('DELETE', $request->getMethod());
        self::assertSame('https://example.test/repos/app-1/credentials/42', (string) $request->getUri());
    }
}
