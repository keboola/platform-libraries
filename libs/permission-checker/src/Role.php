<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

enum Role: string
{
    case NONE = 'none';
    case ADMIN = 'admin';
    case GUEST = 'guest';
    case READ_ONLY = 'readOnly';
    case SHARE = 'share';
    case DEVELOPER = 'developer';
    case REVIEWER = 'reviewer';
    case PRODUCTION_MANAGER = 'productionManager';
}
