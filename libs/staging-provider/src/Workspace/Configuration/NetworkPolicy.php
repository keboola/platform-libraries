<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Workspace\Configuration;

enum NetworkPolicy: string
{
    case SYSTEM = 'system';
    case USER = 'user';
}
