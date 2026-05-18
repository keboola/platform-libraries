<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\SandboxesService;

use Keboola\PermissionChecker\Check\SandboxesService\CanManageAppRepoCredentials;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanManageAppRepoCredentialsTest extends TestCase
{
    public function testAllowManageRepoCredentials(): void
    {
        $this->expectNotToPerformAssertions();

        $appId = 1;
        $projectId = '123';

        $checker = new CanManageAppRepoCredentials($appId, $projectId);
        $checker->checkPermissions(new StorageApiToken(
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
            projectId: $tokenProjectId,
            isAdminToken: true,
        ));
    }
}
