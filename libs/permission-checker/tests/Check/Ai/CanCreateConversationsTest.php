<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\Ai;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\Ai\CanCreateConversations;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanCreateConversationsTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
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

        yield 'token with protected default branch - developer on dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::DEVELOPER->value,
            ),
        ];

        yield 'token with protected default branch - reviewer on dev branch' => [
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

        $checker = new CanCreateConversations($branchType);
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'read-only role' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                role: Role::READ_ONLY->value,
            ),
            'error' => PermissionDeniedException::roleDenied(Role::READ_ONLY, 'create AI conversations'),
        ];

        yield 'production manager on dev branch of protected default branch project' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::PRODUCTION_MANAGER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to create AI conversations on dev branch',
            ),
        ];

        yield 'token with protected default branch - production manager on default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::PRODUCTION_MANAGER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to create AI conversations on default branch',
            ),
        ];

        yield 'developer on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::DEVELOPER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "developer" is not allowed to create AI conversations on default branch',
            ),
        ];

        yield 'reviewer on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::REVIEWER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "reviewer" is not allowed to create AI conversations on default branch',
            ),
        ];

        yield 'none role on protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::NONE->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "none" is not allowed to create AI conversations on default branch',
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

        $checker = new CanCreateConversations($branchType);
        $checker->checkPermissions($token);
    }
}
