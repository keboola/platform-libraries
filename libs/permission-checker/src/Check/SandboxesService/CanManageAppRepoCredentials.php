<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\SandboxesService;

use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\StorageApiToken;

class CanManageAppRepoCredentials implements PermissionCheckInterface
{
    public function __construct(
        private readonly int $appId,
        private readonly string $appProjectId,
    ) {
    }

    public function checkPermissions(StorageApiToken $token): void
    {
        if (!$token->isAdminToken()) {
            throw new PermissionDeniedException(sprintf(
                'Token is not authorized to manage credentials of app \'%s\', admin context is required',
                $this->appId,
            ));
        }

        if (!$token->isForProject($this->appProjectId)) {
            throw new PermissionDeniedException(sprintf(
                'Token is not authorized to manage credentials of app \'%s\', app is from different project',
                $this->appId,
            ));
        }
    }
}
