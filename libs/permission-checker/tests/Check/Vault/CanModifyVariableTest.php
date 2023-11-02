<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\Vault;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\Vault\CanModifyVariable;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanModifyVariableTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        yield 'regular user without branch' => [
            'branchType' => null,
            'token' => new StorageApiToken(role: 'admin'),
        ];

        yield 'regular user on default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(role: 'admin'),
        ];

        yield 'regular user on dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(role: 'admin'),
        ];

        yield 'developer role on protected dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'developer',
            ),
        ];

        yield 'reviewer role on protected dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'reviewer',
            ),
        ];

        yield 'productionManager role on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'productionManager',
            ),
        ];

        yield 'productionManager role on protected-default-branch (without branch)' => [
            'branchType' => null,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'productionManager',
            ),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        ?BranchType $branchType,
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanModifyVariable($branchType);
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'no role' => [
            'branchType' => null,
            'token' => new StorageApiToken(role: null),
            'error' => PermissionDeniedException::roleDenied(Role::NONE, 'modify variable'),
        ];

        yield 'readOnly role' => [
            'branchType' => null,
            'token' => new StorageApiToken(role: 'readOnly'),
            'error' => PermissionDeniedException::roleDenied(Role::READ_ONLY, 'modify variable'),
        ];

        yield 'simple token on protected-default-branch' => [
            'branchType' => null,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
            ),
            'error' => new PermissionDeniedException(
                'Role "none" is not allowed to modify variables without branch',
            ),
        ];

        yield 'developer role on protected-default-branch (without branch)' => [
            'branchType' => null,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'developer',
            ),
            'error' => new PermissionDeniedException(
                'Role "developer" is not allowed to modify variables without branch',
            ),
        ];

        yield 'developer role on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'developer',
            ),
            'error' => new PermissionDeniedException(
                'Role "developer" is not allowed to modify variables on default branch',
            ),
        ];

        yield 'reviewer role on protected-default-branch (without branch)' => [
            'branchType' => null,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'reviewer',
            ),
            'error' => new PermissionDeniedException(
                'Role "reviewer" is not allowed to modify variables without branch',
            ),
        ];

        yield 'reviewer role on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'reviewer',
            ),
            'error' => new PermissionDeniedException(
                'Role "reviewer" is not allowed to modify variables on default branch',
            ),
        ];

        yield 'productionManager role on protected default branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: ['protected-default-branch'],
                role: 'productionManager',
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to modify variables on dev branch',
            ),
        ];
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        ?BranchType $branchType,
        StorageApiToken $token,
        PermissionDeniedException $error,
    ): void {
        $this->expectExceptionObject($error);

        $checker = new CanModifyVariable($branchType);
        $checker->checkPermissions($token);
    }
}
