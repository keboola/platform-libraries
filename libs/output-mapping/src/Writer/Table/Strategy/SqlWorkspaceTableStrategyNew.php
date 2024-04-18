<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;

class SqlWorkspaceTableStrategyNew extends AbstractWorkspaceTableStrategyNew
{
    /**
     * @param MappingFromRawConfiguration[] $configurations
     */
    public function listSources(string $dir, array $configurations): array
    {
        $sources = [];
        foreach ($configurations as $mapping) {
            $source = $mapping->getSourceName();
            $sources[$source] = new WorkspaceItemSource($source, $this->dataStorage->getWorkspaceId(), $source, false);
        }
        return $sources;
    }
}
