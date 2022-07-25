<?php

namespace Keboola\StagingProvider\Staging\Workspace;

class SynapseWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'synapse';
    }
}
