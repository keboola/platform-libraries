<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider\Configuration;

enum NetworkPolicy: string
{
    case SYSTEM = 'system';
    case USER = 'user';
}
