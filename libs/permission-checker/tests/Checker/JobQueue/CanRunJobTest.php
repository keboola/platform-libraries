<?php

declare(strict_types=1);

namespace Keboola\PersmissionChecker\Tests\Checker\JobQueue;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Checker\JobQueue\CanRunJob;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanRunJobTest extends TestCase
{
    public static function provideValidPermissionsCheckData(): iterable
    {
        yield 'simple token' => [
            'branchType' => BranchType::DEFAULT,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2'],
            ),
        ];

        yield 'token with limited components' => [
            'branchType' => BranchType::DEFAULT,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2'],
                allowedComponents: ['keboola.component']
            ),
        ];
    }

    /** @dataProvider provideValidPermissionsCheckData */
    public function testValidPermissionsCheck(
        BranchType $branchType,
        string $componentId,
        StorageApiToken $token,
    ): void {
        $this->expectNotToPerformAssertions();

        $checker = new CanRunJob($branchType, $componentId);
        $checker->checkPermissions($token);
    }

    public static function provideInvalidPermissionsCheckData(): iterable
    {
        yield 'missing Q2 feature' => [
            'branchType' => BranchType::DEFAULT,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(),
            'error' => PermissionDeniedException::missingFeature(Feature::QUEUE_V2),
        ];

        yield 'non-allowed component' => [
            'branchType' => BranchType::DEFAULT,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2'],
                allowedComponents: ['keboola.other-component'],
            ),
            'error' => PermissionDeniedException::missingComponent('keboola.component'),
        ];

        yield 'read-only role' => [
            'branchType' => BranchType::DEFAULT,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2'],
                role: 'readOnly',
            ),
            'error' => PermissionDeniedException::roleDenied(Role::READ_ONLY, 'run jobs'),
        ];

        yield 'production manager on dev branch of protected default branch project' => [
            'branchType' => BranchType::DEV,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2', 'protected-default-branch'],
                role: 'productionManager',
            ),
            'error' => new PermissionDeniedException(
                'Role "productionManager" is not allowed to run jobs on dev branch',
            ),
        ];

        yield 'developer on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2', 'protected-default-branch'],
                role: 'developer',
            ),
            'error' => new PermissionDeniedException('Role "developer" is not allowed to run jobs on default branch'),
        ];

        yield 'reviewer on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2', 'protected-default-branch'],
                role: 'reviewer',
            ),
            'error' => new PermissionDeniedException('Role "reviewer" is not allowed to run jobs on default branch'),
        ];

        yield 'regular token on default branch of protected default branch project' => [
            'branchType' => BranchType::DEFAULT,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2', 'protected-default-branch'],
            ),
            'error' => new PermissionDeniedException('Role "none" is not allowed to run jobs on default branch'),
        ];

        yield 'regular token on dev branch of protected default branch project' => [
            'branchType' => BranchType::DEV,
            'componentId' => 'keboola.component',
            'token' => new StorageApiToken(
                features: ['queuev2', 'protected-default-branch'],
            ),
            'error' => new PermissionDeniedException('Role "none" is not allowed to run jobs on dev branch'),
        ];
    }

    /** @dataProvider provideInvalidPermissionsCheckData */
    public function testInvalidPermissionsCheck(
        BranchType $branchType,
        string $componentId,
        StorageApiToken $token,
        PermissionDeniedException $error,
    ): void {
        $this->expectExceptionObject($error);

        $checker = new CanRunJob($branchType, $componentId);
        $checker->checkPermissions($token);
    }
}
