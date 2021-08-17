<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use SplFileInfo;

abstract class AbstractWorkspaceTableStrategy extends AbstractTableStrategy
{
    public function resolveMappingSources($sourcePathPrefix, array $configuration)
    {
        /** @var array<string, MappingSource> $sources */
        $sources = [];

        // add output mappings from configuration
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $sources[$mapping['source']] = $this->createMapping(
                    $sourcePathPrefix,
                    $mapping['source'],
                    null,
                    $mapping
                );
            }
        }

        // add manifest files
        $sourcesPath = $this->metadataStorage->getPath() . '/' . $sourcePathPrefix;
        $manifestFiles = ManifestHelper::getManifestFiles($sourcesPath);
        foreach ($manifestFiles as $file) {
            $sourceName = $file->getBasename('.manifest');

            if (isset($sources[$sourceName])) {
                $sources[$sourceName]->setManifestFile($file);
            } else {
                $sources[$sourceName] = $this->createMapping($sourcePathPrefix, $sourceName, $file, null);
            }
        }

        return array_values($sources);
    }

    /**
     * @param string $sourcePathPrefix
     * @param string $sourceId
     * @param null|SplFileInfo $manifestFile
     * @param null|array $mapping
     * @return MappingSource
     */
    abstract protected function createMapping($sourcePathPrefix, $sourceId, $manifestFile, $mapping);

    public function loadDataIntoTable($sourceId, $tableId, array $options)
    {
        return new LoadTable($this->clientWrapper->getBasicClient(), $tableId, [
            'dataWorkspaceId' => $this->dataStorage->getWorkspaceId(),
            'dataObject' => $sourceId,
            'incremental' => $options['incremental'],
            'columns' => $options['columns'],
        ]);
    }
}
