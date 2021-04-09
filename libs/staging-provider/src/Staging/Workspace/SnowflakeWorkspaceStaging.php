<?php

namespace Keboola\WorkspaceProvider\Staging\Workspace;

class SnowflakeWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'snowflake';
    }
}
