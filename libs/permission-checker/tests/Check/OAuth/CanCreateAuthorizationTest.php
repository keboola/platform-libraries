<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\OAuth;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\OAuth\CanCreateAuthorization;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanCreateAuthorizationTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        yield 'regular user on regular project default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(role: 'admin'),
        ];

        yield 'regular user on regular project dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(role: 'admin'),
        ];

        yield 'regular user on regular project without branch' => [
            'branchType' => null,
            'token' => new StorageApiToken(role: 'admin'),
        ];

        yield 'guest user' => [
            'branchType' => null,
            'token' => new StorageApiToken(role: 'guest'),
        ];

        yield 'no role' => [
            'branchType' => null,
            'token' => new StorageApiToken(role: null),
        ];

        yield 'productionManager on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'productionManager'),
        ];

        yield 'productionManager without branch (project-wide)' => [
            'branchType' => null,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'productionManager'),
        ];

        yield 'developer on protected dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'developer'),
        ];

        yield 'reviewer on protected dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'reviewer'),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        ?BranchType $branchType,
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanCreateAuthorization($branchType);
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'readOnly role' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(role: 'readOnly'),
            'error' => new PermissionDeniedException('Role "readOnly" is not allowed to create authorization'),
        ];

        yield 'regular token on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: null),
            'error' => new PermissionDeniedException('Role "none" is not allowed to create authorization'),
        ];

        yield 'developer role on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'developer'),
            'error' => new PermissionDeniedException('Role "developer" is not allowed to create authorization'),
        ];

        yield 'developer role without branch' => [
            'branchType' => null,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'developer'),
            'error' => new PermissionDeniedException('Role "developer" is not allowed to create authorization'),
        ];

        yield 'reviewer role on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'reviewer'),
            'error' => new PermissionDeniedException('Role "reviewer" is not allowed to create authorization'),
        ];

        yield 'reviewer role without branch' => [
            'branchType' => null,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'reviewer'),
            'error' => new PermissionDeniedException('Role "reviewer" is not allowed to create authorization'),
        ];

        yield 'productionManager role on protected dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'productionManager'),
            'error' => new PermissionDeniedException('Role "productionManager" is not allowed to create authorization'),
        ];
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        ?BranchType $branchType,
        StorageApiToken $token,
        PermissionDeniedException $error,
    ): void {
        $this->expectExceptionObject($error);

        $checker = new CanCreateAuthorization($branchType);
        $checker->checkPermissions($token);
    }
}
