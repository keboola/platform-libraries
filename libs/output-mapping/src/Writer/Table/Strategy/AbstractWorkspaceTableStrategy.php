<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;

abstract class AbstractWorkspaceTableStrategy extends AbstractTableStrategy
{
    public function resolveMappingSources(string $sourcePathPrefix, array $configuration): array
    {
        $sourcesPath = Path::join($this->metadataStorage->getPath(), $sourcePathPrefix);
        $manifestFiles = FilesHelper::getManifestFiles($sourcesPath);

        /** @var array<string, MappingSource> $mappingSources */
        $mappingSources = [];

        // Create MappingSource for each mapping row. This is workaround for not being able to list real list of tables
        // from workspace.
        foreach ($configuration['mapping'] ?? [] as $mapping) {
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
            $configuration['mapping'] ?? []
        );
    }

    abstract protected function createSource(string $sourcePathPrefix, string $sourceName): WorkspaceItemSource;

    /**
     * @return array {
     *      dataWorkspaceId: string,
     *      dataObject: string
     * }
     */
    public function prepareLoadTaskOptions(SourceInterface $source, array $config): array
    {
        if (!$source instanceof WorkspaceItemSource) {
            throw new InvalidArgumentException(sprintf(
                'Argument $source is expected to be instance of %s, %s given',
                WorkspaceItemSource::class,
                get_class($source)
            ));
        }

        return [
            'dataWorkspaceId' => (string) $source->getWorkspaceId(),
            'dataObject' => (string) $source->getDataObject(),
        ];
    }
}
