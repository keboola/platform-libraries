<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Tests\Check\SandboxesService;

use Keboola\PermissionChecker\Check\SandboxesService\CanManageApp;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiToken;
use PHPUnit\Framework\TestCase;

class CanManageAppTest extends TestCase
{
    public function testAllowManageApp(): void
    {
        $this->expectNotToPerformAssertions();

        $appId = 1;
        $projectId = '123'; // app project id is the same as token project id

        $checker = new CanManageApp($appId, $projectId);
        $checker->checkPermissions(new StorageApiToken(projectId: $projectId));
    }

    public function testDenyManageApp(): void
    {
        $appId = 1;
        $appProjectId = '456';
        $tokenProjectId = '123';

        $this->expectExceptionObject(new PermissionDeniedException(
            'Token is not authorized to manage app \'1\', app is from different project',
        ));

        $checker = new CanManageApp($appId, $appProjectId);
        $checker->checkPermissions(new StorageApiToken(projectId: $tokenProjectId));
    }
}
