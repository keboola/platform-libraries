<?php

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;

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
     * @return array
     */
    public function resolveLoadTaskOptions($sourceId, array $config, array $systemMetadata);
}
