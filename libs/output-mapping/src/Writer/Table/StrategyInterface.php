<?php

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;

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
     * @param bool $destinationTableExists
     * @return LoadTableTaskInterface
     */
    public function prepareLoadTask(
        $sourceId,
        MappingDestination $destination,
        $destinationTableExists,
        array $config,
        array $loadOptions
    );
}
