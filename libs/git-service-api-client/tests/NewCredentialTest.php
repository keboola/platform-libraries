<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests;

use Keboola\GitServiceApiClient\CredentialType;
use Keboola\GitServiceApiClient\KeyPermission;
use Keboola\GitServiceApiClient\NewCredential;
use PHPUnit\Framework\TestCase;
use Webmozart\Assert\InvalidArgumentException;

class NewCredentialTest extends TestCase
{
    public function testSshKey(): void
    {
        $request = NewCredential::sshKey('svc', 'ssh-ed25519 AAA', KeyPermission::ReadWrite);

        self::assertSame(CredentialType::SshKey, $request->type);
        self::assertSame('svc', $request->username);
        self::assertSame('ssh-ed25519 AAA', $request->publicKey);
        self::assertSame(KeyPermission::ReadWrite, $request->permissions);
        self::assertSame(
            [
                'type' => 'ssh_key',
                'username' => 'svc',
                'permissions' => 'readWrite',
                'publicKey' => 'ssh-ed25519 AAA',
            ],
            $request->toRequestBody(),
        );
    }

    public function testHttpToken(): void
    {
        $request = NewCredential::httpToken('bot', KeyPermission::ReadOnly);

        self::assertSame(CredentialType::HttpToken, $request->type);
        self::assertSame('bot', $request->username);
        self::assertNull($request->publicKey);
        self::assertSame(KeyPermission::ReadOnly, $request->permissions);
        self::assertSame(
            [
                'type' => 'http_token',
                'username' => 'bot',
                'permissions' => 'readOnly',
            ],
            $request->toRequestBody(),
        );
    }

    public function testSshKeyRejectsEmptyUsername(): void
    {
        $this->expectException(InvalidArgumentException::class);
        /** @phpstan-ignore argument.type */
        NewCredential::sshKey('', 'ssh-ed25519 AAA', KeyPermission::ReadOnly);
    }

    public function testSshKeyRejectsEmptyPublicKey(): void
    {
        $this->expectException(InvalidArgumentException::class);
        /** @phpstan-ignore argument.type */
        NewCredential::sshKey('svc', '', KeyPermission::ReadOnly);
    }

    public function testHttpTokenRejectsEmptyUsername(): void
    {
        $this->expectException(InvalidArgumentException::class);
        /** @phpstan-ignore argument.type */
        NewCredential::httpToken('', KeyPermission::ReadOnly);
    }
}
