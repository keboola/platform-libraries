<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Reader\Options\InputTableOptions;

interface StrategyInterface
{
    public function downloadTable(InputTableOptions $table);
    public function handleExports($exports);
}
