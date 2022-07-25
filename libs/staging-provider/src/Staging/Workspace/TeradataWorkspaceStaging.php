<?php

namespace Keboola\StagingProvider\Staging\Workspace;

class TeradataWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'teradata';
    }
}
