<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

class Snowflake extends AbstractWorkspaceStrategy
{
    public function getWorkspaceType(): string
    {
        return 'snowflake';
    }
}
