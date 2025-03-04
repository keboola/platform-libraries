<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\Ai;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\Ai\CanModifyConversations;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanModifyConversationsTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        yield 'simple token' => [
            'token' => new StorageApiToken(),
        ];

        yield 'admin token' => [
            'token' => new StorageApiToken(
                role: Role::ADMIN->value,
            ),
        ];

        yield 'share token' => [
            'token' => new StorageApiToken(
                role: Role::SHARE->value,
            ),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanModifyConversations();
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'read-only role' => [
            'token' => new StorageApiToken(
                role: Role::READ_ONLY->value,
            ),
            'error' => PermissionDeniedException::roleDenied(Role::READ_ONLY, 'create AI conversations'),
        ];

        yield 'token with protected default branch' => [
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
            ),
            'error' => new PermissionDeniedException(
                'Role "none" is not allowed to create AI conversations on protected branch projects',
            ),
        ];

        yield 'production manager with protected default branch' => [
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::PRODUCTION_MANAGER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to create AI conversations on protected branch projects',
            ),
        ];

        yield 'developer with protected default branch' => [
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::DEVELOPER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "developer" is not allowed to create AI conversations on protected branch projects',
            ),
        ];

        yield 'reviewer with protected default branch' => [
            'token' => new StorageApiToken(
                features: [Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::REVIEWER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "reviewer" is not allowed to create AI conversations on protected branch projects',
            ),
        ];
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        StorageApiToken $token,
        PermissionDeniedException $error,
    ): void {
        $this->expectExceptionObject($error);

        $checker = new CanModifyConversations();
        $checker->checkPermissions($token);
    }
}
