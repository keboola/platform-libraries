<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Attribute;

class StorageApiTokenRole
{
    public const ROLE_ADMIN = 1;
    public const ROLE_GUEST = 2;
    public const ROLE_READ_ONLY = 4;
    public const ROLE_SHARE = 8;
    public const ROLE_DEVELOPER = 16;
    public const ROLE_REVIEWER = 32;
    public const ROLE_PRODUCTION_MANAGER = 64;

    public const ANY =
        self::ROLE_ADMIN |
        self::ROLE_GUEST |
        self::ROLE_READ_ONLY |
        self::ROLE_SHARE |
        self::ROLE_DEVELOPER |
        self::ROLE_REVIEWER |
        self::ROLE_PRODUCTION_MANAGER
    ;

    private const MAP = [
        'admin' => self::ROLE_ADMIN,
        'guest' => self::ROLE_GUEST,
        'readonly' => self::ROLE_READ_ONLY,
        'share' => self::ROLE_SHARE,
        'developer' => self::ROLE_DEVELOPER,
        'reviewer' => self::ROLE_REVIEWER,
        'production-manager' => self::ROLE_PRODUCTION_MANAGER,
    ];

    public static function rolesToMask(array $roles): int
    {
        return array_reduce(
            $roles,
            fn(int $sum, string $role) => $sum + (self::MAP[$role] ?? 0),
            0,
        );
    }

    public static function maskToRoles(int $sum): array
    {
        $roles = [];
        foreach (self::MAP as $role => $value) {
            if (($sum & $value) === $value) {
                $roles[] = $role;
            }
        }

        return $roles;
    }
}
