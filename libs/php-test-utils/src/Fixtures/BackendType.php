<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures;

enum BackendType: string
{
    case BIGQUERY = 'bigquery';
    case SNOWFLAKE = 'snowflake';
}
