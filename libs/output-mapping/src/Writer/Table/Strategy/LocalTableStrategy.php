<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Table\Source\FileSource;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

class LocalTableStrategy extends AbstractTableStrategy
{
    public function resolveMappings($sourcePath, array $configuration)
    {
        /** @var SourceInterface[] $sources */
        $sources = [];

        $dataFiles = ManifestHelper::getNonManifestFiles($sourcePath);
        foreach ($dataFiles as $file) {
            $sources[$file->getBasename()] = new FileSource($file);
        }

        $manifestFiles = ManifestHelper::getManifestFiles($sourcePath);
        foreach ($manifestFiles as $file) {
            $dataFileName = $file->getBasename('.manifest');

            if (!isset($sources[$dataFileName])) {
                throw new InvalidOutputException(sprintf('Found orphaned table manifest: "%s"', $file->getBasename()));
            }

            $sources[$dataFileName]->setManifestFile($file);
        }

        // Check if all files from output mappings are present
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $filename = $mapping['source'];

                if (!isset($sources[$filename])) {
                    throw new InvalidOutputException(sprintf('Table source "%s" not found.', $mapping['source']), 404);
                }
            }

            $sources = $this->combineSourcesWithMapping($sources, $configuration['mapping']);
        }

        return array_values($sources);
    }

    /**
     * @param SourceInterface[] $sources
     * @param array<array{source: string}> $mappings
     * @return SourceInterface[]
     */
    private function combineSourcesWithMapping(array $sources, array $mappings)
    {
        $mappingsBySource = [];
        foreach ($mappings as $mapping) {
            $mappingsBySource[$mapping['source']][] = $mapping;
        }

        $sourcesWithMapping = [];
        foreach ($sources as $source) {
            $sourceMappings = isset($mappingsBySource[$source->getSourceName()]) ?
                $mappingsBySource[$source->getSourceName()] :
                []
            ;

            if (count($sourceMappings) === 0) {
                $sourcesWithMapping[] = $source;
                continue;
            }

            foreach ($sourceMappings as $sourceMapping) {
                $sourceCopy = clone $source;
                $sourceCopy->setMapping($sourceMapping);
                $sourcesWithMapping[] = $sourceCopy;
            }
        }

        return $sourcesWithMapping;
    }
}
