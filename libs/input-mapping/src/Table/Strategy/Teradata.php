<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

class Teradata extends AbstractDatabaseStrategy
{
    protected function getWorkspaceType(): string
    {
        return 'teradata';
    }
}
