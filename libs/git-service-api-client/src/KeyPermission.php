<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

enum KeyPermission: string
{
    case ReadOnly = 'readOnly';
    case ReadWrite = 'readWrite';
}
