<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\SandboxesService;

use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\StorageApiToken;

class CanManageApp implements PermissionCheckInterface
{
    public function __construct(
        private readonly string $appId,
        private readonly string $appProjectId,
    ) {
    }

    public function checkPermissions(StorageApiToken $token): void
    {
        if (!$token->isForProject($this->appProjectId)) {
            throw new PermissionDeniedException(sprintf(
                'Token is not authorized to manage app \'%s\', app is from different project',
                $this->appId,
            ));
        }
    }
}
