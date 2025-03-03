<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\Ai;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;

class CanCreateConversations implements PermissionCheckInterface
{
    public function __construct(
        private readonly BranchType $branchType,
    ) {
    }

    public function checkPermissions(StorageApiToken $token): void
    {
        if ($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH)) {
            $this->checkProtectedDefaultBranch($token);
        } elseif ($token->isRole(Role::READ_ONLY)) {
            throw PermissionDeniedException::roleDenied($token->getRole(), 'create AI conversations');
        }
    }

    private function checkProtectedDefaultBranch(StorageApiToken $token): void
    {
        $isAllowed = match ($token->getRole()) {
            Role::DEVELOPER, Role::REVIEWER => $this->branchType !== BranchType::DEFAULT,
            default => false,
        };

        if (!$isAllowed) {
            throw new PermissionDeniedException(sprintf(
                'Role "%s" is not allowed to create AI conversations on %s branch',
                $token->getRole()->value,
                $this->branchType->value,
            ));
        }
    }
}
