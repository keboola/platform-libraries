<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\StorageApiBranch\StorageApiToken as BaseStorageApiToken;

class PermissionChecker
{
    /**
     * @throws PermissionDeniedException
     */
    public function checkPermissions(BaseStorageApiToken $token, PermissionCheckInterface $checker): void
    {
        $adaptedStorageToken = StorageApiToken::fromTokenInterface($token);
        $checker->checkPermissions($adaptedStorageToken);
    }
}
