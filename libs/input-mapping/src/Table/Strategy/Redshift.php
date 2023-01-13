<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

class Redshift extends AbstractDatabaseStrategy
{
    protected function getWorkspaceType(): string
    {
        return 'redshift';
    }
}
