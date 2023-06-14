<?php

declare(strict_types=1);

namespace Keboola\PermissionChecker;

/**
 * Available roles according to
 * https://github.com/keboola/connection/blob/master/legacy-app/library/Keboola/Validate/ProjectRole.php
 */
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
