<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

class BigQuery extends AbstractWorkspaceStrategy
{
    protected function getWorkspaceType(): string
    {
        return 'bigquery';
    }
}
