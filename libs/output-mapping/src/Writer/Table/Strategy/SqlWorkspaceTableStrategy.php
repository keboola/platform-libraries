<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;

class SqlWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    protected function createSource($sourcePathPrefix, $sourceName)
    {
        return new WorkspaceItemSource(
            $sourceName,
            (string) $this->dataStorage->getWorkspaceId(),
            $sourceName,
            false
        );
    }
}
