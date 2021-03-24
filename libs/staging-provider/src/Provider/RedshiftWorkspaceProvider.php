<?php

namespace Keboola\WorkspaceProvider\Provider;

class RedshiftWorkspaceProvider extends AbstractWorkspaceProvider
{
    protected function getType()
    {
        return 'redshift';
    }
}
