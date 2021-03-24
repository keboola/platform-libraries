<?php

namespace Keboola\WorkspaceProvider\Provider;

class SnowflakeWorkspaceProvider extends AbstractWorkspaceProvider
{
    protected function getType()
    {
        return 'snowflake';
    }
}
