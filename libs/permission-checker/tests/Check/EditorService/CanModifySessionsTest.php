<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\EditorService;

use Generator;
use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\EditorService\CanModifySessions;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanModifySessionsTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): Generator
    {
        yield 'simple token' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(),
        ];

        yield 'admin token' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                role: Role::ADMIN->value,
            ),
        ];

        yield 'share token' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                role: Role::SHARE->value,
            ),
        ];

        yield 'developer on dev branch in protected project' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::DEVELOPER->value,
            ),
        ];

        yield 'reviewer on dev branch in protected project' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::REVIEWER->value,
            ),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        BranchType $branchType,
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanModifySessions($branchType);
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): Generator
    {
        yield 'read-only role' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                role: Role::READ_ONLY->value,
            ),
            'error' => PermissionDeniedException::roleDenied(Role::READ_ONLY, 'modify sessions'),
        ];

        yield 'production manager on default branch in protected project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::PRODUCTION_MANAGER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to modify sessions on default branch',
            ),
        ];

        yield 'production manager on dev branch in protected project' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::PRODUCTION_MANAGER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to modify sessions on dev branch',
            ),
        ];

        yield 'developer on default branch in protected project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::DEVELOPER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "developer" is not allowed to modify sessions on default branch',
            ),
        ];

        yield 'reviewer on default branch in protected project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::REVIEWER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "reviewer" is not allowed to modify sessions on default branch',
            ),
        ];
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        BranchType $branchType,
        StorageApiToken $token,
        PermissionDeniedException $error,
    ): void {
        $this->expectExceptionObject($error);

        $checker = new CanModifySessions($branchType);
        $checker->checkPermissions($token);
    }
}
