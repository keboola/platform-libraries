<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\SandboxesService;

use Keboola\PermissionChecker\Check\SandboxesService\CanManageAppRepoCredentials;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanManageAppRepoCredentialsTest extends TestCase
{
    public static function provideAllowedRoles(): iterable
    {
        yield 'admin' => ['role' => 'admin'];
        yield 'guest' => ['role' => 'guest'];
        yield 'share' => ['role' => 'share'];
        yield 'developer' => ['role' => 'developer'];
        yield 'reviewer' => ['role' => 'reviewer'];
        yield 'productionManager' => ['role' => 'productionManager'];
    }

    /** @dataProvider provideAllowedRoles */
    public function testAllowManageRepoCredentials(string $role): void
    {
        $this->expectNotToPerformAssertions();

        $appId = 1;
        $projectId = '123';

        $checker = new CanManageAppRepoCredentials($appId, $projectId);
        $checker->checkPermissions(new StorageApiToken(
            role: $role,
            projectId: $projectId,
            isAdminToken: true,
        ));
    }

    public static function provideDeniedRoles(): iterable
    {
        yield 'no role' => [
            'role' => null,
            'error' => new PermissionDeniedException('Role "none" is not allowed to manage credentials of app \'1\''),
        ];

        yield 'readOnly' => [
            'role' => 'readOnly',
            'error' => new PermissionDeniedException(
                'Role "readOnly" is not allowed to manage credentials of app \'1\'',
            ),
        ];
    }

    /** @dataProvider provideDeniedRoles */
    public function testDenyRole(?string $role, PermissionDeniedException $error): void
    {
        $appId = 1;
        $projectId = '123';

        $this->expectExceptionObject($error);

        $checker = new CanManageAppRepoCredentials($appId, $projectId);
        $checker->checkPermissions(new StorageApiToken(
            role: $role,
            projectId: $projectId,
            isAdminToken: true,
        ));
    }

    public function testDenyNonAdminToken(): void
    {
        $appId = 1;
        $projectId = '123';

        $this->expectExceptionObject(new PermissionDeniedException(
            'Token is not authorized to manage credentials of app \'1\', admin context is required',
        ));

        $checker = new CanManageAppRepoCredentials($appId, $projectId);
        $checker->checkPermissions(new StorageApiToken(
            role: 'admin',
            projectId: $projectId,
            isAdminToken: false,
        ));
    }

    public function testDenyDifferentProject(): void
    {
        $appId = 1;
        $appProjectId = '456';
        $tokenProjectId = '123';

        $this->expectExceptionObject(new PermissionDeniedException(
            'Token is not authorized to manage credentials of app \'1\', app is from different project',
        ));

        $checker = new CanManageAppRepoCredentials($appId, $appProjectId);
        $checker->checkPermissions(new StorageApiToken(
            role: 'admin',
            projectId: $tokenProjectId,
            isAdminToken: true,
        ));
    }
}
