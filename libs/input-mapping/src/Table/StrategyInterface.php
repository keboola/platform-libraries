<?php

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\Table\Options\InputTableOptions;

interface StrategyInterface
{
    public function downloadTable(InputTableOptions $table);
    public function handleExports($exports);
}
