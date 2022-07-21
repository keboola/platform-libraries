<?php

namespace Keboola\InputMapping\Table\Strategy;

class Redshift extends AbstractDatabaseStrategy
{
    protected function getWorkspaceType()
    {
        return 'redshift';
    }
}
