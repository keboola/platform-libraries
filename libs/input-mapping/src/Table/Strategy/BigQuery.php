<?php

namespace Keboola\InputMapping\Table\Strategy;

class BigQuery extends AbstractDatabaseStrategy
{
    protected function getWorkspaceType()
    {
        return 'bigquery';
    }
}
