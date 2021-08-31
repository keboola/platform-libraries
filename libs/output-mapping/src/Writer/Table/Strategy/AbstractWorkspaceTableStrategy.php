<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;

abstract class AbstractWorkspaceTableStrategy extends AbstractTableStrategy
{
    public function resolveMappingSources($sourcePathPrefix, array $configuration)
    {
        $sourcesPath = Path::join($this->metadataStorage->getPath(), $sourcePathPrefix);
        $manifestFiles = FilesHelper::getManifestFiles($sourcesPath);

        /** @var array<string, MappingSource> $mappingSources */
        $mappingSources = [];

        // Create MappingSource for each mapping row. This is workaround for not being able to list real list of tables
        // from workspace.
        foreach (isset($configuration['mapping']) ? $configuration['mapping'] : [] as $mapping) {
            $sourceName = $mapping['source'];
            $source = $this->createSource($sourcePathPrefix, $sourceName);
            $mappingSources[$sourceName] = new MappingSource($source);
        }

        foreach ($manifestFiles as $file) {
            $sourceName = $file->getBasename('.manifest');

            if (!isset($mappingSources[$sourceName])) {
                $source = $this->createSource($sourcePathPrefix, $sourceName);
                $mappingSources[$sourceName] = new MappingSource($source);
            }

            $mappingSources[$sourceName]->setManifestFile($file);
        }

        return $this->combineSourcesWithMappingsFromConfiguration(
            $mappingSources,
            isset($configuration['mapping']) ? $configuration['mapping'] : []
        );
    }

    /**
     * @param string $sourcePathPrefix
     * @param string $sourceName
     * @return WorkspaceItemSource
     */
    abstract protected function createSource($sourcePathPrefix, $sourceName);

    public function prepareLoadTaskOptions(SourceInterface $source, array $config)
    {
        if (!$source instanceof WorkspaceItemSource) {
            throw new InvalidArgumentException(sprintf(
                'Argument $source is expected to be instance of %s, %s given',
                WorkspaceItemSource::class,
                get_class($source)
            ));
        }

        return [
            'dataWorkspaceId' => $source->getWorkspaceId(),
            'dataObject' => $source->getDataObject(),
        ];
    }
}
