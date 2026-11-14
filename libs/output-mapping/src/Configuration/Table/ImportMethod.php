<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table;

/**
 * How Storage writes a Snowflake destination table on import (`importMethod` of the import-async API).
 * AUTO is the platform default, so it is never sent to the API.
 */
enum ImportMethod: string
{
    case AUTO = 'auto';
    case SINGLE_STATEMENT = 'single_statement';
    case MULTI_STATEMENT = 'multi_statement';
}
