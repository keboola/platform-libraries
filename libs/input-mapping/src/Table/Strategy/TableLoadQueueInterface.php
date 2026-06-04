<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;

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
}
