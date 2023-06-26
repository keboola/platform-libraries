<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker\Check\Scheduler;

use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\PermissionCheckInterface;
use Keboola\PermissionChecker\StorageApiToken;

class CanViewSchedules implements PermissionCheckInterface
{
    public function checkPermissions(StorageApiToken $token): void
    {
    }
}
