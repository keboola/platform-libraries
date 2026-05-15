<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Model;

use Keboola\GitServiceApiClient\CredentialType;
use Keboola\GitServiceApiClient\KeyPermission;
use Keboola\GitServiceApiClient\Model\Credential;
use PHPUnit\Framework\TestCase;
use ValueError;
use Webmozart\Assert\InvalidArgumentException;

class CredentialTest extends TestCase
{
    public function testFromResponseDataSshKey(): void
    {
        $credential = Credential::fromResponseData([
            'id' => '42',
            'type' => 'ssh_key',
            'username' => 'myapp-my-service',
            'publicKey' => 'ssh-ed25519 AAAA...',
            'permissions' => 'readOnly',
            'createdAt' => '2026-04-28T10:00:00Z',
        ]);

        self::assertSame('42', $credential->id);
        self::assertSame(CredentialType::SshKey, $credential->type);
        self::assertSame('myapp-my-service', $credential->username);
        self::assertSame('ssh-ed25519 AAAA...', $credential->publicKey);
        self::assertSame(KeyPermission::ReadOnly, $credential->permissions);
        self::assertSame('2026-04-28T10:00:00Z', $credential->createdAt);
    }

    public function testFromResponseDataHttpToken(): void
    {
        $credential = Credential::fromResponseData([
            'id' => '43',
            'type' => 'http_token',
            'username' => 'myapp-my-service',
            'permissions' => 'readWrite',
            'createdAt' => '2026-04-28T10:00:00Z',
        ]);

        self::assertSame(CredentialType::HttpToken, $credential->type);
        self::assertNull($credential->publicKey);
        self::assertSame(KeyPermission::ReadWrite, $credential->permissions);
    }

    public function testFromResponseDataInvalidType(): void
    {
        $this->expectException(ValueError::class);
        Credential::fromResponseData([
            'id' => '44',
            'type' => 'bogus',
            'username' => 'u',
            'permissions' => 'readOnly',
            'createdAt' => 't',
        ]);
    }

    public function testFromResponseDataInvalidPermission(): void
    {
        $this->expectException(ValueError::class);
        Credential::fromResponseData([
            'id' => '45',
            'type' => 'ssh_key',
            'username' => 'u',
            'permissions' => 'admin',
            'createdAt' => 't',
        ]);
    }

    public function testFromResponseDataMissingRequiredField(): void
    {
        $this->expectException(InvalidArgumentException::class);
        Credential::fromResponseData([
            'id' => '46',
            'type' => 'ssh_key',
            // username missing
            'permissions' => 'readOnly',
            'createdAt' => 't',
        ]);
    }
}
