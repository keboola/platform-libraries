<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;

interface StrategyInterface
{
    public function downloadTable(RewrittenInputTableOptions $table): array;

    public function handleExports(array $exports, bool $preserve): array;
}
