<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

enum TokenPermission: string
{
    case CAN_CREATE_JOBS = 'canCreateJobs';
    case CAN_MANAGE_TOKENS = 'canManageTokens';
}
