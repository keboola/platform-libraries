<?php

namespace Keboola\StagingProvider\Staging\Workspace;

class SnowflakeWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'snowflake';
    }
}
