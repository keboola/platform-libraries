<?php

namespace Keboola\StagingProvider\Staging\Workspace;

class ExasolWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'exasol';
    }
}
