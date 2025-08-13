<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

enum DeduplicationStrategy: string
{
    case INSERT = 'insert';
    case UPSERT = 'upsert';
}
