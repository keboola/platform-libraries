<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\Ai;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;

class CanCreateAutomations implements PermissionCheckInterface
{
    public function __construct(
        private readonly BranchType $branchType,
    ) {
    }

    public function checkPermissions(StorageApiToken $token): void
    {
        if (!$token->hasFeature(Feature::AI_AUTOMATIONS)) {
            throw PermissionDeniedException::missingFeature(Feature::AI_AUTOMATIONS);
        }

        if ($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH)) {
            $this->checkProtectedDefaultBranch($token);
        } elseif ($token->isRole(Role::READ_ONLY)) {
            throw PermissionDeniedException::roleDenied($token->getRole(), 'create AI automations');
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
                'Role "%s" is not allowed to create AI automations on %s branch',
                $token->getRole()->value,
                $this->branchType->value,
            ));
        }
    }
}
