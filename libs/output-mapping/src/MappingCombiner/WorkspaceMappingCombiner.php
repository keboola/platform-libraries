<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\MappingCombiner;

use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalData;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use function Aws\manifest;

class WorkspaceMappingCombiner implements MappingCombinerInterface
{
    public function __construct(readonly private string $workspaceId)
    {
    }

    /**
     * @param array<SourceInterface> $dataItems
     * @param array<MappingFromRawConfiguration> $configurations
     * @return array<MappingFromRawConfigurationAndPhysicalData>
     */
    public function combineDataItemsWithConfigurations(array $dataItems, array $configurations): array
    {
        $combinedSources = [];
        /** @var FileItem $dataItem */
        foreach ($dataItems as $dataItem) {
            $configurationMappings = array_filter(
                $configurations,
                fn(MappingFromRawConfiguration $mapping) => $mapping->getSourceName() === $dataItem->getName(),
            );

            /** @var MappingFromRawConfiguration|null $configurationMapping */
            foreach ($configurationMappings ?: [null] as $configurationMapping) {
                $combinedSources[] = new MappingFromRawConfigurationAndPhysicalData(
                    $dataItem,
                    $configurationMapping,
                );
            }
        }

        return $combinedSources;
    }

    /**
     * @param MappingFromRawConfigurationAndPhysicalData[] $sources
     * @param FileItem[] $manifests
     * @return MappingFromRawConfigurationAndPhysicalDataWithManifest[]
     */
    public function combineSourcesWithManifests(array $sources, array $manifests): array
    {
        $manifestsWithoutSource = $manifests;
        $combinedSources = [];
        foreach ($sources as $source) {
            $sourceKey = array_search($source->getManifestName(), array_map(fn($v) => $v->getName(), $manifests));

            $combinedSources[] = new MappingFromRawConfigurationAndPhysicalDataWithManifest(
                $source,
                $sourceKey !== false ? $manifests[$sourceKey] : null,
            );
            unset($manifestsWithoutSource[$sourceKey]);
        }

        foreach ($manifestsWithoutSource as $manifest) {
            $sourceName = basename($manifest->getName(), '.manifest');
            $source = new WorkspaceItemSource(
                $sourceName,
                $this->workspaceId,
                $sourceName,
                false,
            );

            $combinedSources[] = new MappingFromRawConfigurationAndPhysicalDataWithManifest(
                new MappingFromRawConfigurationAndPhysicalData($source, null),
                $manifest,
            );
        }
        return $combinedSources;
    }
}
