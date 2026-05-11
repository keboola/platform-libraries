<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient\Tests\Model;

use Keboola\GitServiceApiClient\KeyPermission;
use Keboola\GitServiceApiClient\Model\DeployKey;
use PHPUnit\Framework\TestCase;
use ValueError;
use Webmozart\Assert\InvalidArgumentException;

class DeployKeyTest extends TestCase
{
    public function testFromResponseDataReadOnly(): void
    {
        $key = DeployKey::fromResponseData([
            'id' => 'key-1',
            'createdAt' => '2026-04-28T10:00:00Z',
            'permissions' => 'readOnly',
        ]);

        self::assertSame('key-1', $key->id);
        self::assertSame('2026-04-28T10:00:00Z', $key->createdAt);
        self::assertSame(KeyPermission::ReadOnly, $key->permissions);
    }

    public function testFromResponseDataReadWrite(): void
    {
        $key = DeployKey::fromResponseData([
            'id' => 'key-2',
            'createdAt' => '2026-04-28T10:00:00Z',
            'permissions' => 'readWrite',
        ]);

        self::assertSame(KeyPermission::ReadWrite, $key->permissions);
    }

    public function testFromResponseDataInvalidPermission(): void
    {
        $this->expectException(ValueError::class);
        DeployKey::fromResponseData([
            'id' => 'key-3',
            'createdAt' => '2026-04-28T10:00:00Z',
            'permissions' => 'admin',
        ]);
    }

    public function testFromResponseDataMissingField(): void
    {
        $this->expectException(InvalidArgumentException::class);
        DeployKey::fromResponseData([
            'id' => 'key-4',
            'createdAt' => '2026-04-28T10:00:00Z',
            // permissions missing
        ]);
    }
}
