<?php

namespace Keboola\InputMapping\Table\Strategy;

class Teradata extends AbstractDatabaseStrategy
{
    protected function getWorkspaceType()
    {
        return 'teradata';
    }
}
