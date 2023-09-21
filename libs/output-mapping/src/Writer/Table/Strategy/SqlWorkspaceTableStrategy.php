<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;

class SqlWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    protected function createSource(string $sourcePathPrefix, string $sourceName): WorkspaceItemSource
    {
        return new WorkspaceItemSource(
            $sourceName,
            $this->dataStorage->getWorkspaceId(),
            $sourceName,
            false,
        );
    }
}
