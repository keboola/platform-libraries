<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use Keboola\GitServiceApiClient\CredentialType;
use Keboola\GitServiceApiClient\GitServiceApiClient;
use Keboola\GitServiceApiClient\KeyPermission;
use Keboola\GitServiceApiClient\NewCredential;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

class GitServiceApiClientTest extends TestCase
{
    /**
     * @param list<MockResponse> $responses
     */
    private function buildClient(array $responses): GitServiceApiClient
    {
        return new GitServiceApiClient(
            'https://example.test',
            manageToken: 'token',
            httpClient: new MockHttpClient($responses, 'https://example.test/'),
        );
    }

    public function testCreateRepository(): void
    {
        $response = new MockResponse((string) json_encode([
            'name' => 'app-1',
            'createdAt' => '2026-04-28T10:00:00Z',
            'defaultBranch' => 'main',
            'sshUrl' => 'ssh://git/app-1',
            'httpsUrl' => 'https://git/app-1.git',
        ]), ['http_code' => 201]);
        $client = $this->buildClient([$response]);

        $repo = $client->createRepository('app-1');

        self::assertSame('app-1', $repo->name);
        self::assertSame('https://git/app-1.git', $repo->httpsUrl);
        self::assertSame('POST', $response->getRequestMethod());
        self::assertSame('https://example.test/repos', $response->getRequestUrl());
        self::assertSame('{"name":"app-1"}', $response->getRequestOptions()['body'] ?? null);
    }

    public function testGetRepository(): void
    {
        $response = new MockResponse((string) json_encode([
            'name' => 'app-1',
            'createdAt' => 't',
            'defaultBranch' => 'main',
            'sshUrl' => 's',
            'httpsUrl' => 'https://git/app-1.git',
        ]));
        $client = $this->buildClient([$response]);

        $repo = $client->getRepository('app-1');

        self::assertSame('app-1', $repo->name);
        self::assertSame('https://git/app-1.git', $repo->httpsUrl);
        self::assertSame('GET', $response->getRequestMethod());
        self::assertSame('https://example.test/repos/app-1', $response->getRequestUrl());
    }

    public function testGetRepositoryEncodesName(): void
    {
        $response = new MockResponse((string) json_encode([
            'name' => 'app/1', 'createdAt' => 't', 'defaultBranch' => 'main', 'sshUrl' => 's', 'httpsUrl' => 'h',
        ]));
        $client = $this->buildClient([$response]);

        $client->getRepository('app/1');

        self::assertSame('https://example.test/repos/app%2F1', $response->getRequestUrl());
    }

    public function testDeleteRepository(): void
    {
        $response = new MockResponse('', ['http_code' => 204]);
        $client = $this->buildClient([$response]);

        $client->deleteRepository('app-1');

        self::assertSame('DELETE', $response->getRequestMethod());
        self::assertSame('https://example.test/repos/app-1', $response->getRequestUrl());
    }

    public function testListCredentials(): void
    {
        $response = new MockResponse((string) json_encode([
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
        ]));
        $client = $this->buildClient([$response]);

        $credentials = $client->listCredentials('app-1');

        self::assertCount(2, $credentials);
        self::assertSame('1', $credentials[0]->id);
        self::assertSame(CredentialType::SshKey, $credentials[0]->type);
        self::assertSame('ssh-ed25519 AAA', $credentials[0]->publicKey);
        self::assertSame(CredentialType::HttpToken, $credentials[1]->type);
        self::assertNull($credentials[1]->publicKey);
        self::assertSame('GET', $response->getRequestMethod());
        self::assertSame('https://example.test/repos/app-1/credentials', $response->getRequestUrl());
    }

    public function testGetCredential(): void
    {
        $response = new MockResponse((string) json_encode([
            'id' => '42',
            'type' => 'ssh_key',
            'username' => 'app-1-svc',
            'publicKey' => 'ssh-ed25519 AAA',
            'permissions' => 'readWrite',
            'createdAt' => 't',
        ]));
        $client = $this->buildClient([$response]);

        $credential = $client->getCredential('app-1', '42');

        self::assertSame('42', $credential->id);
        self::assertSame(CredentialType::SshKey, $credential->type);
        self::assertSame(KeyPermission::ReadWrite, $credential->permissions);
        self::assertSame('GET', $response->getRequestMethod());
        self::assertSame('https://example.test/repos/app-1/credentials/42', $response->getRequestUrl());
    }

    public function testGetCredentialEncodesIds(): void
    {
        $response = new MockResponse((string) json_encode([
            'id' => 'a/b',
            'type' => 'ssh_key',
            'username' => 'u',
            'publicKey' => 'k',
            'permissions' => 'readOnly',
            'createdAt' => 't',
        ]));
        $client = $this->buildClient([$response]);

        $client->getCredential('app/1', 'a/b');

        self::assertSame(
            'https://example.test/repos/app%2F1/credentials/a%2Fb',
            $response->getRequestUrl(),
        );
    }

    public function testCreateCredentialWithSshKey(): void
    {
        $response = new MockResponse((string) json_encode([
            'id' => '42',
            'type' => 'ssh_key',
            'username' => 'app-1-svc',
            'publicKey' => 'ssh-ed25519 AAA',
            'permissions' => 'readWrite',
            'createdAt' => 't',
        ]), ['http_code' => 201]);
        $client = $this->buildClient([$response]);

        $credential = $client->createCredential(
            'app-1',
            NewCredential::sshKey('svc', 'ssh-ed25519 AAA', KeyPermission::ReadWrite),
        );

        self::assertSame('42', $credential->id);
        self::assertSame(CredentialType::SshKey, $credential->type);
        self::assertSame('ssh-ed25519 AAA', $credential->publicKey);
        self::assertNull($credential->secret);
        self::assertSame('POST', $response->getRequestMethod());
        self::assertSame('https://example.test/repos/app-1/credentials', $response->getRequestUrl());
        self::assertSame(
            '{"type":"ssh_key","username":"svc","permissions":"readWrite","publicKey":"ssh-ed25519 AAA"}',
            $response->getRequestOptions()['body'] ?? null,
        );
    }

    public function testCreateCredentialWithHttpToken(): void
    {
        $response = new MockResponse((string) json_encode([
            'id' => '43',
            'type' => 'http_token',
            'username' => 'app-1-bot',
            'secret' => 'ghs_abc123',
            'httpsUrl' => 'https://forgejo.example.com/keboola/app-1.git',
            'permissions' => 'readOnly',
            'createdAt' => 't',
        ]), ['http_code' => 201]);
        $client = $this->buildClient([$response]);

        $credential = $client->createCredential(
            'app-1',
            NewCredential::httpToken('bot', KeyPermission::ReadOnly),
        );

        self::assertSame('43', $credential->id);
        self::assertSame(CredentialType::HttpToken, $credential->type);
        self::assertSame('ghs_abc123', $credential->secret);
        self::assertSame('https://forgejo.example.com/keboola/app-1.git', $credential->httpsUrl);
        self::assertNull($credential->publicKey);
        self::assertSame('POST', $response->getRequestMethod());
        self::assertSame('https://example.test/repos/app-1/credentials', $response->getRequestUrl());
        self::assertSame(
            '{"type":"http_token","username":"bot","permissions":"readOnly"}',
            $response->getRequestOptions()['body'] ?? null,
        );
    }

    public function testDeleteCredential(): void
    {
        $response = new MockResponse('', ['http_code' => 204]);
        $client = $this->buildClient([$response]);

        $client->deleteCredential('app-1', '42');

        self::assertSame('DELETE', $response->getRequestMethod());
        self::assertSame('https://example.test/repos/app-1/credentials/42', $response->getRequestUrl());
    }
}
