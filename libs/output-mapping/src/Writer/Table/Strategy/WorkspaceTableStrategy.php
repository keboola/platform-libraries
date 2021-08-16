<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Table\Source\TableSource;

class WorkspaceTableStrategy extends AbstractTableStrategy
{
    public function resolveMappings($sourcePath, array $configuration)
    {
        /** @var array<string, TableSource> $sourcesManifests */
        $sourcesManifests = [];

        // add output mappings fom configuration
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $sourcesManifests[$mapping['source']] = new TableSource(
                    $mapping['source'],
                    null,
                    $mapping
                );
            }
        }

        // add manifest files
        $manifestFiles = ManifestHelper::getManifestFiles($sourcePath);
        foreach ($manifestFiles as $file) {
            $sourceName = $file->getBasename('.manifest');

            if (isset($sourcesManifests[$sourceName])) {
                $sourcesManifests[$sourceName]->setManifestFile($file);
            } else {
                $sourcesManifests[$sourceName] = new TableSource($sourceName, $file, null);
            }
        }

        return $sourcesManifests;
    }
}
