<?php

declare(strict_types=1);

namespace Keboola\K8sClient\BaseApi\Data;

enum WatchEventType: string
{
    case Added = 'ADDED';
    case Modified = 'MODIFIED';
    case Deleted = 'DELETED';
    case Error = 'ERROR';
}
