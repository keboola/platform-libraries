<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

use Keboola\PermissionChecker\Exception\PermissionDeniedException;

interface PermissionCheckInterface
{
    /**
     * Checks if the token has permissions to perform the action.
     *
     * Each action should implement its own checker. If the token does not have permissions to perform the action,
     * the checker throws PermissionDeniedException.
     *
     * @throws PermissionDeniedException
     */
    public function checkPermissions(StorageApiToken $token): void;
}
