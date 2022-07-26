<?php

namespace Keboola\StagingProvider\Staging\Workspace;

class RedshiftWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'redshift';
    }
}
