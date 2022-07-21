<?php

namespace Keboola\InputMapping\Table\Strategy;

class Snowflake extends AbstractDatabaseStrategy
{
    protected function getWorkspaceType()
    {
        return 'snowflake';
    }
}
