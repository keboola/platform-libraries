<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Writer\Table\MappingSource;

class SqlWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    protected function createMapping($sourcePathPrefix, $sourceId, $manifestFile, $mapping)
    {
        return new MappingSource($sourceId, $sourceId, $manifestFile, $mapping);
    }
}
