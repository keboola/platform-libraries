<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\Vault;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;

class CanModifyVariable implements PermissionCheckInterface
{
    public function __construct(
        private readonly BranchType $branchType,
    ) {
    }

    public function checkPermissions(StorageApiToken $token): void
    {
        if ($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH)) {
            $this->checkProtectedDefaultBranch($token->getRole());
        } elseif ($token->isRole(Role::READ_ONLY)) {
            throw PermissionDeniedException::roleDenied($token->getRole(), 'modify variable');
        }
    }

    private function checkProtectedDefaultBranch(Role $role): void
    {
        $isAllowed = match ($role) {
            Role::PRODUCTION_MANAGER => $this->branchType === BranchType::DEFAULT,
            Role::DEVELOPER, Role::REVIEWER => $this->branchType === BranchType::DEV,
            default => false,
        };

        if (!$isAllowed) {
            throw new PermissionDeniedException(sprintf(
                'Role "%s" is not allowed to modify variables on %s branch',
                $role->value,
                $this->branchType->value,
            ));
        }
    }
}
