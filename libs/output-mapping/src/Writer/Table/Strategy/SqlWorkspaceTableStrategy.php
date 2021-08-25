<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Writer\Table\MappingSource;

class SqlWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    protected function createMapping($sourcePathPrefix, $sourceName, $manifestFile, $mapping)
    {
        return new MappingSource($sourceName, $sourceName, $manifestFile, $mapping);
    }
}
