<?php

declare(strict_types=1);

namespace Keboola\ApiBundle;

enum ServiceDnsType: string
{
    case INTERNAL = 'internal';
    case PUBLIC = 'public';
}
