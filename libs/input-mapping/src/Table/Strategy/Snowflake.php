<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

class Snowflake extends AbstractWorkspaceStrategy
{
    protected function getWorkspaceType(): string
    {
        return 'snowflake';
    }
}
