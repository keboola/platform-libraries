<?php

namespace Keboola\WorkspaceProvider\Staging\Workspace;

class SynapseWorkspaceStaging extends WorkspaceStaging
{
    public static function getType()
    {
        return 'synapse';
    }
}
