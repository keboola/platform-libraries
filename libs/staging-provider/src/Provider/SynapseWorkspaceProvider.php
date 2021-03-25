<?php

namespace Keboola\WorkspaceProvider\Provider;

class SynapseWorkspaceProvider extends AbstractWorkspaceProvider
{
    protected function getType()
    {
        return 'synapse';
    }
}
