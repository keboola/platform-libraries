<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Reader\LoadTypeDecider;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;

class SnowflakeStrategy extends RedshiftStrategy
{
    public function downloadTable(InputTableOptions $table)
    {
        $tableInfo = $this->storageClient->getTable($table->getSource());
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        if (LoadTypeDecider::canClone($tableInfo, 'snowflake', $loadOptions)) {
            $this->logger->info(sprintf('Table "%s" will be cloned.', $table->getSource()));
            $workspaceClones[WorkspaceProviderInterface::TYPE_SNOWFLAKE][] = $table;
        } else {
            $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
            $workspaceCopies[WorkspaceProviderInterface::TYPE_SNOWFLAKE][] = [$table, $loadOptions];
        }
    }
}
