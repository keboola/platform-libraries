<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests\Checker\Notification;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Checker\Notification\CanModifySubscriptions;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanModifySubscriptionsTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        yield 'regular token on regular project default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(role: null),
        ];

        yield 'regular token on regular project dev branch' => [
            'branchType' => BranchType::DEV,
            'token' => new StorageApiToken(role: null),
        ];

        yield 'productionManager on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(role: 'productionManager'),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        BranchType $branchType,
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanModifySubscriptions($branchType);
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'guest role' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(role: 'guest'),
            'error' => new PermissionDeniedException('Role "guest" is not allowed to modify subscriptions'),
        ];

        yield 'readOnly role' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(role: 'readOnly'),
            'error' => new PermissionDeniedException('Role "readOnly" is not allowed to modify subscriptions'),
        ];

        yield 'regular token on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: null),
            'error' => new PermissionDeniedException('Role "none" is not allowed to modify subscriptions'),
        ];

        yield 'developer role on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'developer'),
            'error' => new PermissionDeniedException('Role "developer" is not allowed to modify subscriptions'),
        ];

        yield 'reviewer role on protected default branch' => [
            'branchType' => BranchType::DEFAULT,
            'token' => new StorageApiToken(features: ['protected-default-branch'], role: 'reviewer'),
            'error' => new PermissionDeniedException('Role "reviewer" is not allowed to modify subscriptions'),
        ];
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        BranchType $branchType,
        StorageApiToken $token,
        PermissionDeniedException $error,
    ): void {
        $this->expectExceptionObject($error);

        $checker = new CanModifySubscriptions($branchType);
        $checker->checkPermissions($token);
    }
}
