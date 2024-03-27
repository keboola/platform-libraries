<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Attribute;

use InvalidArgumentException;
use Keboola\ApiBundle\Attribute\StorageApiTokenAuth;
use PHPUnit\Framework\TestCase;

class StorageApiTokenAuthTest extends TestCase
{
    public function testInvalidNonAdminTokenWithRoleFails(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid combination of role AND isAdmin=false. Only admin tokens has roles');

        new StorageApiTokenAuth(isAdmin: false, role: 1);
    }
}
