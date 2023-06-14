<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

use Keboola\PermissionChecker\Exception\PermissionDeniedException;

interface PermissionCheckerInterface
{
    /**
     * @throws PermissionDeniedException
     */
    public function checkPermissions(StorageApiToken $token): void;
}
