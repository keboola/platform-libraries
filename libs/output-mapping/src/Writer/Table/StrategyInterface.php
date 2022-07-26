<?php

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;
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
     * @return MappingSource[]
     */
    public function resolveMappingSources($sourcePathPrefix, array $configuration);

    /**
     * @return array
     */
    public function prepareLoadTaskOptions(SourceInterface $source, array $config);
}
