<?php

namespace Keboola\StagingProvider\Staging\Workspace;

class BigQueryWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'bigquery';
    }
}
