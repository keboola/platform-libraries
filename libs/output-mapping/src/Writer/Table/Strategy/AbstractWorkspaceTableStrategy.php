<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\DeferredTasks\TableWriterV2\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriterV2\LoadTableTask;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Symfony\Component\Finder\SplFileInfo;

abstract class AbstractWorkspaceTableStrategy extends AbstractTableStrategy
{
    public function resolveMappingSources($sourcePathPrefix, array $configuration)
    {
        $sourcesPath = Path::join($this->metadataStorage->getPath(), $sourcePathPrefix);
        $manifestFiles = FilesHelper::getManifestFiles($sourcesPath);

        /** @var array<string, MappingSource> $sources */
        $sources = [];

        foreach (isset($configuration['mapping']) ? $configuration['mapping'] : [] as $mapping) {
            $sourceName = $mapping['source'];

            // Create empty mapping. This is workaround for not being able to list real list of tables from workspace.
            $sources[$sourceName] = $this->createMapping($sourcePathPrefix, $sourceName, null, null);
        }

        foreach ($manifestFiles as $file) {
            $sourceName = $file->getBasename('.manifest');

            if (isset($sources[$sourceName])) {
                $sources[$sourceName]->setManifestFile($file);
            } else {
                $sources[$sourceName] = $this->createMapping($sourcePathPrefix, $sourceName, $file, null);
            }
        }

        return $this->combineSourcesWithMappingsFromConfiguration(
            $sources,
            isset($configuration['mapping']) ? $configuration['mapping'] : []
        );
    }

    /**
     * @param string $sourcePathPrefix
     * @param string $sourceName
     * @param null|SplFileInfo $manifestFile
     * @param null|array $mapping
     * @return MappingSource
     */
    abstract protected function createMapping($sourcePathPrefix, $sourceName, $manifestFile, $mapping);

    public function prepareLoadTask(
        $sourceId,
        MappingDestination $destination,
        $destinationTableExists,
        array $config,
        array $loadOptions
    ) {
        $loadOptions = array_merge($loadOptions, [
            'dataWorkspaceId' => $this->dataStorage->getWorkspaceId(),
            'dataObject' => $sourceId,
        ]);

        if ($destinationTableExists) {
            return new LoadTableTask($destination, $loadOptions);
        }

        return new CreateAndLoadTableTask($destination, $loadOptions);
    }
}
