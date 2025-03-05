<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\Ai;

use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\Feature;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\Role;
use Keboola\PermissionChecker\StorageApiToken;

class CanModifyConversations implements PermissionCheckInterface
{
    public function checkPermissions(StorageApiToken $token): void
    {
        if ($token->hasFeature(Feature::PROTECTED_DEFAULT_BRANCH)) {
            throw new PermissionDeniedException(sprintf(
                'Role "%s" is not allowed to create AI conversations on protected branch projects',
                $token->getRole()->value,
            ));
        } elseif ($token->isRole(Role::READ_ONLY)) {
            throw PermissionDeniedException::roleDenied($token->getRole(), 'create AI conversations');
        }
    }
}
