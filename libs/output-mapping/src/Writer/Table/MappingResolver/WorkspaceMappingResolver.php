<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\MappingResolver;

use Exception;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\MappingSource;

class WorkspaceMappingResolver extends AbstractMappingResolver
{
    public function __construct(private readonly string $path) {}

    public function resolveMappingSources(string $sourcePathPrefix, array $configuration, bool $isFailedJob): array
    {
        $sourcesPath = Path::join($this->path, $sourcePathPrefix);
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
            $configuration['mapping'] ?? [],
            $isFailedJob
        );
    }
}
