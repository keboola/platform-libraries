<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests\Check\Scheduler;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\Scheduler\CanMigrateSchedules;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanMigrateSchedulesTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        /** @var (BranchType|null)[] $branchTypes */
        $branchTypes = [null, BranchType::DEFAULT, BranchType::DEV];

        // standard projects - allowed for none, share and admin roles for all branch types
        foreach ($branchTypes as $branchType) {
            $label = $branchType ? sprintf(' on %s branch', $branchType->value) : '';

            yield 'regular token' . $label => [
                'token' => new StorageApiToken(role: null),
                'branchType' => $branchType,
            ];

            yield 'none role' . $label => [
                'token' => new StorageApiToken(role: 'none'),
                'branchType' => $branchType,
            ];

            yield 'share role' . $label => [
                'token' => new StorageApiToken(role: 'share'),
                'branchType' => $branchType,
            ];

            yield 'admin role' . $label  => [
                'token' => new StorageApiToken(role: 'admin'),
                'branchType' => $branchType,
            ];
        }

        // sox projects
        yield 'sox productionManager role' => [
            'token' => new StorageApiToken(role: 'productionManager', features: ['protected-default-branch']),
            'branchType' => null,
        ];

        yield 'sox productionManager role on default branch' => [
            'token' => new StorageApiToken(role: 'productionManager', features: ['protected-default-branch']),
            'branchType' => BranchType::DEFAULT,
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        StorageApiToken $token,
        ?BranchType $branchType,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanMigrateSchedules($branchType);
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        /** @var (BranchType|null)[] $branchTypes */
        $branchTypes = [null, BranchType::DEFAULT, BranchType::DEV];

        $roles = [null, 'none', 'guest', 'readOnly', 'admin', 'share', 'developer', 'reviewer', 'productionManager'];

        $errorPattern = 'Role "%s" is insufficient for this operation.';

        // standard projects
        foreach ($branchTypes as $branchType) {
            foreach ($roles as $role) {
                if (in_array($role, [null, 'none', 'admin', 'share'], true)) {
                    continue;
                }

                $label = $role;
                $label .= $branchType ? sprintf(' on %s branch', $branchType->value) : '';

                yield $label  => [
                    'token' => new StorageApiToken(role: $role),
                    'branchType' => $branchType,
                    'errorMessage' => sprintf($errorPattern, $role),
                ];
            }
        }

        // sox projects
        foreach ($branchTypes as $branchType) {
            foreach ($roles as $role) {
                if ($role === 'productionManager' && (!$branchType || $branchType->value === 'default')) {
                    continue;
                }

                $label = 'sox ';
                $label .= $role ?: 'regular token';
                $label .= $branchType ? sprintf(' on %s branch', $branchType->value) : '';

                yield $label => [
                    'token' => new StorageApiToken(role: $role, features: ['protected-default-branch']),
                    'branchType' => $branchType,
                    'errorMessage' => sprintf($errorPattern, $role ?: 'none'),
                ];
            }
        }
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        StorageApiToken $token,
        ?BranchType $branchType,
        string $errorMessage,
    ): void {
        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage($errorMessage);

        $checker = new CanMigrateSchedules($branchType);
        $checker->checkPermissions($token);
    }
}
