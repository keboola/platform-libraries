<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Model;

use Keboola\GitServiceApiClient\CredentialType;
use Keboola\GitServiceApiClient\KeyPermission;
use Keboola\GitServiceApiClient\Model\CreatedCredential;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class CreatedCredentialTest extends TestCase
{
    public function testFromResponseDataHttpToken(): void
    {
        $credential = CreatedCredential::fromResponseData([
            'id' => '42',
            'type' => 'http_token',
            'username' => 'myapp-my-service',
            'secret' => 'ghs_abc123',
            'httpsUrl' => 'https://forgejo.example.com/keboola/myapp.git',
            'permissions' => 'readWrite',
            'createdAt' => '2026-04-28T10:00:00Z',
        ]);

        self::assertSame('42', $credential->id);
        self::assertSame(CredentialType::HttpToken, $credential->type);
        self::assertSame('myapp-my-service', $credential->username);
        self::assertNull($credential->publicKey);
        self::assertSame('ghs_abc123', $credential->secret);
        self::assertSame('https://forgejo.example.com/keboola/myapp.git', $credential->httpsUrl);
        self::assertSame(KeyPermission::ReadWrite, $credential->permissions);
        self::assertSame('2026-04-28T10:00:00Z', $credential->createdAt);
    }

    public function testFromResponseDataSshKey(): void
    {
        $credential = CreatedCredential::fromResponseData([
            'id' => '43',
            'type' => 'ssh_key',
            'username' => 'myapp-my-service',
            'publicKey' => 'ssh-ed25519 AAAA...',
            'permissions' => 'readOnly',
            'createdAt' => '2026-04-28T10:00:00Z',
        ]);

        self::assertSame(CredentialType::SshKey, $credential->type);
        self::assertSame('ssh-ed25519 AAAA...', $credential->publicKey);
        self::assertNull($credential->secret);
        self::assertNull($credential->httpsUrl);
        self::assertSame(KeyPermission::ReadOnly, $credential->permissions);
    }

    public function testFromResponseDataMissingRequiredField(): void
    {
        $this->expectException(InvalidArgumentException::class);
        CreatedCredential::fromResponseData([
            'id' => '44',
            'type' => 'http_token',
            'username' => 'u',
            // permissions missing
            'createdAt' => 't',
        ]);
    }
}
