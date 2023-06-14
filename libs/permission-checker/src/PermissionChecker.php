<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

use Keboola\PermissionChecker\Exception\PermissionDeniedException;

class PermissionChecker
{
    /**
     * @throws PermissionDeniedException
     */
    public function checkPermissions(StorageApiTokenInterface $token, PermissionCheckerInterface $checker): void
    {
        $adaptedStorageToken = StorageApiToken::fromTokenInterface($token);
        $checker->checkPermissions($adaptedStorageToken);
    }
}
