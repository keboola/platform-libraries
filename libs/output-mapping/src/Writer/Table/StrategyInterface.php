<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Writer\Table\MappingResolver\MappingResolverInterface;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

interface StrategyInterface
{
    public function getDataStorage(): ProviderInterface;

    public function getMappingResolver(): MappingResolverInterface;

    public function getMetadataStorage(): ProviderInterface;

    public function prepareLoadTaskOptions(SourceInterface $source, array $config): array;

}
