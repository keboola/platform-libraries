<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\TableSource;

class WorkspaceTableStrategy extends AbstractTableStrategy
{
    public function resolveMappings($sourcePathPrefix, array $configuration)
    {
        /** @var array<string, TableSource> $sourcesManifests */
        $sourcesManifests = [];

        // add output mappings fom configuration
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $sourcesManifests[$mapping['source']] = new TableSource(
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

            if (isset($sourcesManifests[$sourceName])) {
                $sourcesManifests[$sourceName]->setManifestFile($file);
            } else {
                $sourcesManifests[$sourceName] = new TableSource($sourcePathPrefix, $sourceName, $file, null);
            }
        }

        return $sourcesManifests;
    }

    public function loadDataIntoTable(SourceInterface $source, $tableId, array $options)
    {
        return new LoadTable($this->clientWrapper->getBasicClient(), $tableId, [
            'dataWorkspaceId' => $this->dataStorage->getWorkspaceId(),
            'dataObject' => $source->getSourceId(),
            'incremental' => $options['incremental'],
            'columns' => $options['columns'],
        ]);
    }
}
