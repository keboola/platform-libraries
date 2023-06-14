<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

enum BranchType: string
{
    case DEFAULT = 'default';
    case DEV = 'dev';
}
