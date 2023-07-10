<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests\Check\RunnerSyncApi;

use Generator;
use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\RunnerSyncApi\CanRunAction;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanRunActionTest extends TestCase
{
    public function branchTypeProvider(): Generator
    {
        foreach (BranchType::cases() as $branchType) {
            yield $branchType->value => [$branchType];
        }
    }

    /**
     * @dataProvider branchTypeProvider
     */
    public function testPermissionCheckForTokensWithoutPermissionToComponent(BranchType $branchType): void
    {
        $token = new StorageApiToken(
            allowedComponents: []
        );

        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage('Token is not allowed to run component "dummy-component"');

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($token);
    }

    /**
     * @dataProvider branchTypeProvider
     */
    public function testPermissionCheckFailOnReadOnlyRole(BranchType $branchType): void
    {
        $token = new StorageApiToken(
            role: Role::READ_ONLY->value,
            allowedComponents: ['dummy-component']
        );

        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage('Role "readOnly" is not allowed to run actions');

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($token);
    }

    public function validPermissionsCheckProvider(): Generator
    {
        foreach (BranchType::cases() as $branchType) {
            foreach (Role::cases() as $role) {
                if ($role === Role::READ_ONLY) {
                    continue;
                }
                yield $branchType->value . ' branch, ' . $role->value => [
                    'role' => $role,
                    'branchType' => $branchType,
                    'expectedErrorMessage' => sprintf(
                        'Role "%s" is not allowed to run actions on %s branch',
                        $role->value,
                        $branchType->value
                    ),
                ];
            }
        }
    }

    /**
     * @dataProvider validPermissionsCheckProvider
     */
    public function testValidPermissionsCheck(Role $role, BranchType $branchType): void
    {
        $this->expectNotToPerformAssertions();

        $token = new StorageApiToken(
            role: $role !== Role::NONE ? $role->value : null,
            allowedComponents: ['dummy-component']
        );

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($token);
    }

    public function permissionCheckFailOnSoxProjectsProvider(): Generator
    {
        foreach (BranchType::cases() as $branchType) {
            foreach (Role::cases() as $role) {
                if ($role === Role::PRODUCTION_MANAGER && $branchType === BranchType::DEFAULT) {
                    continue;
                }
                if ($role === Role::DEVELOPER && $branchType === BranchType::DEV) {
                    continue;
                }
                if ($role === Role::REVIEWER && $branchType === BranchType::DEV) {
                    continue;
                }

                yield $branchType->value . ' branch, ' . $role->value => [
                    'role' => $role,
                    'branchType' => $branchType,
                    'expectedErrorMessage' => sprintf(
                        'Role "%s" is not allowed to run actions on %s branch',
                        $role->value,
                        $branchType->value
                    ),
                ];
            }
        }
    }

    /**
     * @dataProvider permissionCheckFailOnSoxProjectsProvider
     */
    public function testPermissionCheckFailOnSoxProjects(
        Role $role,
        BranchType $branchType,
        string $expectedErrorMessage
    ): void {
        $token = new StorageApiToken(
            features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
            role: $role !== Role::NONE ? $role->value : null,
            allowedComponents: ['dummy-component']
        );

        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage($expectedErrorMessage);

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($token);
    }

    public function validPermissionsCheckOnSoxProjectsProvider(): Generator
    {
        yield 'default branch, productionManager' => [
            'role' => Role::PRODUCTION_MANAGER,
            'branchType' => BranchType::DEFAULT,
        ];
        yield 'dev branch, developer' => [
            'role' => Role::DEVELOPER,
            'branchType' => BranchType::DEV,
        ];
        yield 'dev branch, reviewer' => [
            'role' => Role::REVIEWER,
            'branchType' => BranchType::DEV,
        ];
    }

    /**
     * @dataProvider validPermissionsCheckOnSoxProjectsProvider
     */
    public function testValidPermissionsCheckOnSoxProjects(Role $role, BranchType $branchType): void
    {
        $this->expectNotToPerformAssertions();

        $token = new StorageApiToken(
            features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
            role: $role !== Role::NONE ? $role->value : null,
            allowedComponents: ['dummy-component']
        );

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($token);
    }
}
