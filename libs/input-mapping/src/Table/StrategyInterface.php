<?php

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\Table\Options\InputTableOptions;

interface StrategyInterface
{
    public function downloadTable(InputTableOptions $table);
    /**
     * @param array $exports
     * @param bool $preserve
     * @return array
     */
    public function handleExports($exports, $preserve);
}
