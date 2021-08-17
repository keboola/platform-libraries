<?php

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\Writer\Table\MappingSource;

interface StrategyInterface
{
    /**
     * @return ProviderInterface
     */
    public function getDataStorage();

    /**
     * @return ProviderInterface
     */
    public function getMetadataStorage();

    /**
     * @param string$sourcePathPrefix
     * @param array $configuration
     * @return MappingSource[]
     */
    public function resolveMappingSources($sourcePathPrefix, array $configuration);

    /**
     * @param string $sourceId
     * @param string $tableId
     * @param array $options
     * @return LoadTable
     */
    public function loadDataIntoTable($sourceId, $tableId, array $options);
}
