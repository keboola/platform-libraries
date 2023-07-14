<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

enum TokenPermission: string
{
    case CAN_RUN_JOBS = 'canRunJobs';
    case CAN_MANAGE_TOKENS = 'canManageTokens';
}
