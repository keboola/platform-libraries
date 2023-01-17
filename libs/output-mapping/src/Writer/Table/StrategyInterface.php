<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

interface StrategyInterface
{
    public function getDataStorage(): ProviderInterface;

    public function getMetadataStorage(): ProviderInterface;

    /**
     * @return MappingSource[]
     */
    public function resolveMappingSources(string $sourcePathPrefix, array $configuration): array;

    public function prepareLoadTaskOptions(SourceInterface $source, array $config): array;
}
