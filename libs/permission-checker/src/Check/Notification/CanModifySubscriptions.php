<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\Notification;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;

class CanModifySubscriptions implements PermissionCheckInterface
{
    public function __construct(
        private readonly ?BranchType $branchType,
    ) {
    }

    public function checkPermissions(StorageApiToken $token): void
    {
        if ($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH)) {
            $isRoleAllowed = match ($token->getRole()) {
                Role::PRODUCTION_MANAGER => $this->branchType === BranchType::DEFAULT,
                Role::DEVELOPER, Role::REVIEWER => $this->branchType === BranchType::DEV,
                default => false,
            };
        } else {
            $isRoleAllowed = match ($token->getRole()) {
                Role::NONE, Role::GUEST, Role::READ_ONLY => false,
                default => true,
            };
        }

        if (!$isRoleAllowed) {
            throw PermissionDeniedException::roleDenied($token->getRole(), 'modify subscriptions');
        }
    }
}
