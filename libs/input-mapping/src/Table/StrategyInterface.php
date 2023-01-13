<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\Table\Options\InputTableOptions;

interface StrategyInterface
{
    public function downloadTable(InputTableOptions $table): array;

    public function handleExports(array $exports, bool $preserve): array;
}
