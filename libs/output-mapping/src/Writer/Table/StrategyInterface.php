<?php

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

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
     * @return SourceInterface[]
     */
    public function resolveMappings($sourcePathPrefix, array $configuration);

    /**
     * @param SourceInterface $source
     * @param string $tableId
     * @param array $options
     * @return LoadTable
     */
    public function loadDataIntoTable(SourceInterface $source, $tableId, array $options);
}
