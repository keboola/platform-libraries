<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\Scheduler;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;

class CanModifySchedules implements PermissionCheckInterface
{
    public function checkPermissions(StorageApiToken $token): void
    {
        $role = $token->getRole();
        $isAllowed = match ($role) {
            Role::ADMIN, Role::SHARE => true,
            default => false,
        };

        if (!$isAllowed) {
            throw new PermissionDeniedException(sprintf(
                'Role "%s" is insufficient for this operation.',
                $role->value
            ));
        }
    }
}
