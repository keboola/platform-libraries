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
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['ai-automations'],
            ),
        ];

        yield 'admin token with ai-automations feature' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['ai-automations'],
                role: Role::ADMIN->value,
            ),
        ];

        yield 'share token with ai-automations feature' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['ai-automations'],
                role: Role::SHARE->value,
            ),
        ];

        yield 'developer role on protected dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                role: Role::DEVELOPER->value,
                features: ['ai-automations', 'protected-default-branch'],
            ),
        ];

        yield 'reviewer role on protected dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                role: Role::REVIEWER->value,
                features: ['ai-automations', 'protected-default-branch'],
            ),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        BranchType $branchType,
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanCreateAutomations($branchType);
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'missing ai-automations feature' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(),
            'error' => PermissionDeniedException::missingFeature(Feature::AI_AUTOMATIONS),
        ];

        yield 'read-only role' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['ai-automations'],
                role: Role::READ_ONLY->value,
            ),
            'error' => PermissionDeniedException::roleDenied(Role::READ_ONLY, 'create AI automations'),
        ];

        yield 'production manager on dev branch of protected default branch project' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(
                features: ['ai-automations', 'protected-default-branch'],
                role: Role::PRODUCTION_MANAGER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to create AI automations on dev branch',
            ),
        ];

        yield 'productionManager role on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                role: Role::PRODUCTION_MANAGER->value,
                features: ['ai-automations', 'protected-default-branch'],
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to create AI automations on default branch',
            ),
        ];

        yield 'developer on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['ai-automations', 'protected-default-branch'],
                role: Role::DEVELOPER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "developer" is not allowed to create AI automations on default branch',
            ),
        ];

        yield 'reviewer on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['ai-automations', 'protected-default-branch'],
                role: Role::REVIEWER->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "reviewer" is not allowed to create AI automations on default branch',
            ),
        ];

        yield 'regular token on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['ai-automations', 'protected-default-branch'],
            ),
            'error' => new PermissionDeniedException(
                'Role "none" is not allowed to create AI automations on default branch',
            ),
        ];

        yield 'guest on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(
                features: ['ai-automations', 'protected-default-branch'],
                role: Role::GUEST->value,
            ),
            'error' => new PermissionDeniedException(
                'Role "guest" is not allowed to create AI automations on default branch',
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

        $checker = new CanCreateAutomations($branchType);
        $checker->checkPermissions($token);
    }
}
