<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\Scheduler;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;

class CanModifySchedules implements PermissionCheckInterface
{
    public function __construct(
        private readonly ?BranchType $branchType,
    ) {
    }

    public function checkPermissions(StorageApiToken $token): void
    {
        $role = $token->getRole();
        if ($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH)) {
            $this->checkProtectedDefaultBranch($role);
        } else {
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

    private function checkProtectedDefaultBranch(Role $role): void
    {
        $isAllowed = match ($role) {
            Role::PRODUCTION_MANAGER => $this->branchType === null || $this->branchType === BranchType::DEFAULT,
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
