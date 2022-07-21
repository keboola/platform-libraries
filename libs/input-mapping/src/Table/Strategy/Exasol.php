<?php

namespace Keboola\InputMapping\Table\Strategy;

class Exasol extends AbstractDatabaseStrategy
{
    protected function getWorkspaceType()
    {
        return 'exasol';
    }
}
