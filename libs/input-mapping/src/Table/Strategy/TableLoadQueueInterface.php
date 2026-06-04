<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\StrategyInterface;

interface TableLoadQueueInterface
{
    /**
     * @return array<int|string> Storage job ids to await
     */
    public function getJobIds(): array;

    /**
     * @return RewrittenInputTableOptions[] All tables covered by the queued jobs
     */
    public function getAllTables(): array;

    /**
     * @return class-string<StrategyInterface> Strategy class that created this queue
     */
    public function getStrategyClass(): string;

    /**
     * Destination folder the load was planned for (manifest paths derive from it)
     */
    public function getDestination(): string;
}
