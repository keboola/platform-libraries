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
        $tokenMock = $this->createMock(StorageApiToken::class);
        $tokenMock->expects(self::once())
            ->method('hasAllowedComponent')
            ->with('dummy-component')
            ->willReturn(false)
        ;

        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage('Token is not allowed to run component "dummy-component"');

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($tokenMock);
    }

    /**
     * @dataProvider branchTypeProvider
     */
    public function testPermissionCheckFailOnReadOnlyRole(BranchType $branchType): void
    {
        $tokenMock = $this->createMock(StorageApiToken::class);
        $tokenMock->expects(self::once())
            ->method('hasAllowedComponent')
            ->with('dummy-component')
            ->willReturn(true)
        ;

        $tokenMock->expects(self::once())
            ->method('hasFeature')
            ->with(Feature::PROTECTED_DEFAULT_BRANCH)
            ->willReturn(false)
        ;

        $tokenMock->expects(self::once())
            ->method('isRole')
            ->with(Role::READ_ONLY)
            ->willReturn(true)
        ;

        $tokenMock->expects(self::once())
            ->method('getRole')
            ->willReturn(Role::READ_ONLY)
        ;

        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage('Role "readOnly" is not allowed to run actions');

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($tokenMock);
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
        $tokenMock = $this->createMock(StorageApiToken::class);
        $tokenMock->expects(self::once())
            ->method('hasAllowedComponent')
            ->with('dummy-component')
            ->willReturn(true)
        ;

        $tokenMock->expects(self::once())
            ->method('hasFeature')
            ->with(Feature::PROTECTED_DEFAULT_BRANCH)
            ->willReturn(false)
        ;

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($tokenMock);
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
        $tokenMock = $this->createMock(StorageApiToken::class);
        $tokenMock->expects(self::once())
            ->method('hasAllowedComponent')
            ->with('dummy-component')
            ->willReturn(true)
        ;

        $tokenMock->expects(self::once())
            ->method('hasFeature')
            ->with(Feature::PROTECTED_DEFAULT_BRANCH)
            ->willReturn(true)
        ;

        $tokenMock->expects(self::once())
            ->method('getRole')
            ->willReturn($role)
        ;

        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage($expectedErrorMessage);

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($tokenMock);
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
        $tokenMock = $this->createMock(StorageApiToken::class);
        $tokenMock->expects(self::once())
            ->method('hasAllowedComponent')
            ->with('dummy-component')
            ->willReturn(true)
        ;

        $tokenMock->expects(self::once())
            ->method('hasFeature')
            ->with(Feature::PROTECTED_DEFAULT_BRANCH)
            ->willReturn(true)
        ;

        $tokenMock->expects(self::once())
            ->method('getRole')
            ->willReturn($role)
        ;

        $checker = new CanRunAction($branchType, 'dummy-component');
        $checker->checkPermissions($tokenMock);
    }
}
