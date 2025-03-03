<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\Ai;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Check\Ai\CanCreateAutomations;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanCreateAutomationsTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        yield 'simple token with ai-automations feature' => [
            'token' => new StorageApiToken(
                features: [Feature::AI_AUTOMATIONS->value],
            ),
        ];

        yield 'admin token with ai-automations feature' => [
            'token' => new StorageApiToken(
                features: [Feature::AI_AUTOMATIONS->value],
                role: Role::ADMIN->value,
            ),
        ];

        yield 'share token with ai-automations feature' => [
            'token' => new StorageApiToken(
                features: [Feature::AI_AUTOMATIONS->value],
                role: Role::SHARE->value,
            ),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanCreateAutomations();
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'missing ai-automations feature' => [
            'token' => new StorageApiToken(),
            'error' => PermissionDeniedException::missingFeature(Feature::AI_AUTOMATIONS),
        ];

        yield 'read-only role' => [
            'token' => new StorageApiToken(
                features: [Feature::AI_AUTOMATIONS->value],
                role: Role::READ_ONLY->value,
            ),
            'error' => PermissionDeniedException::roleDenied(Role::READ_ONLY, 'create AI automations'),
        ];

        yield 'token with protected default branch' => [
            'token' => new StorageApiToken(
                features: [Feature::AI_AUTOMATIONS->value, Feature::PROTECTED_DEFAULT_BRANCH->value],
            ),
            'error' => new PermissionDeniedException(
                'Role "none" is not allowed to create AI automations on branch protected projects',
            ),
        ];

        yield 'production manager with protected default branch' => [
            'token' => new StorageApiToken(
                features: [Feature::AI_AUTOMATIONS->value, Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::PRODUCTION_MANAGER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to create AI automations on branch protected projects',
            ),
        ];

        yield 'developer with protected default branch' => [
            'token' => new StorageApiToken(
                features: [Feature::AI_AUTOMATIONS->value, Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::DEVELOPER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "developer" is not allowed to create AI automations on branch protected projects',
            ),
        ];

        yield 'reviewer with protected default branch' => [
            'token' => new StorageApiToken(
                features: [Feature::AI_AUTOMATIONS->value, Feature::PROTECTED_DEFAULT_BRANCH->value],
                role: Role::REVIEWER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "reviewer" is not allowed to create AI automations on branch protected projects',
            ),
        ];
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        StorageApiToken $token,
        PermissionDeniedException $error,
    ): void {
        $this->expectExceptionObject($error);

        $checker = new CanCreateAutomations();
        $checker->checkPermissions($token);
    }
}
