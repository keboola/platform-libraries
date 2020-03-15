<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Reader\LoadTypeDecider;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;

class SnowflakeStrategy extends RedshiftStrategy
{
    protected $workspaceProviderId = WorkspaceProviderInterface::TYPE_SNOWFLAKE;

    public function downloadTable(InputTableOptions $table)
    {
        $tableInfo = $this->storageClient->getTable($table->getSource());
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        if (LoadTypeDecider::canClone($tableInfo, 'snowflake', $loadOptions)) {
            $this->logger->info(sprintf('Table "%s" will be cloned.', $table->getSource()));
            return [
                'table' => $table,
                'type' => 'clone'
            ];
        }
        $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
        return [
            'table' => [$table, $loadOptions],
            'type' => 'copy',
        ];
    }
}
