<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Attribute;

use Keboola\ApiBundle\Attribute\StorageApiTokenRole;
use PHPUnit\Framework\TestCase;

class StorageApiTokenRoleTest extends TestCase
{
    private const ALL_KNOWN_ROLES = [
        'admin',
        'guest',
        'readonly',
        'share',
        'developer',
        'reviewer',
        'production-manager',
    ];

    public function provideRolesMasks(): iterable
    {
        yield 'no role' => [
            'mask' => 0,
            'roles' => [],
        ];

        yield 'single role' => [
            'mask' => StorageApiTokenRole::ROLE_ADMIN,
            'roles' => ['admin'],
        ];

        yield 'multiple roles' => [
            'mask' => StorageApiTokenRole::ROLE_ADMIN | StorageApiTokenRole::ROLE_DEVELOPER,
            'roles' => ['admin', 'developer'],
        ];

        yield 'any role' => [
            'mask' => StorageApiTokenRole::ANY,
            'roles' => self::ALL_KNOWN_ROLES,
        ];

        yield 'any role except one' => [
            'mask' => StorageApiTokenRole::ANY & ~StorageApiTokenRole::ROLE_ADMIN,
            'roles' => array_diff(self::ALL_KNOWN_ROLES, ['admin']),
        ];

        yield 'any role except some' => [
            'mask' => StorageApiTokenRole::ANY & ~StorageApiTokenRole::ROLE_ADMIN & ~StorageApiTokenRole::ROLE_GUEST,
            'roles' => array_diff(self::ALL_KNOWN_ROLES, ['admin', 'guest']),
        ];
    }

    /** @dataProvider provideRolesMasks */
    public function testRolesToMask(int $mask, array $roles): void
    {
        self::assertSame($mask, StorageApiTokenRole::rolesToMask($roles));
    }

    /** @dataProvider provideRolesMasks */
    public function testMaskToRoles(int $mask, array $roles): void
    {
        self::assertSame(array_values($roles), StorageApiTokenRole::maskToRoles($mask));
    }

    public function testInvalidRoleToMask(): void
    {
        self::assertSame(0, StorageApiTokenRole::rolesToMask(['invalid']));
    }
}
