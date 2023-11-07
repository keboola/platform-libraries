<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\MappingResolver;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Helper\SliceHelper;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;

class LocalMappingResolver extends AbstractMappingResolver
{
    public function __construct(private readonly string $path)
    {
    }

    public function resolveMappingSources(string $sourcePathPrefix, array $configuration, bool $isFailedJob): array
    {
        $sourcesPath = Path::join($this->path, $sourcePathPrefix);
        $dataFiles = FilesHelper::getDataFiles($sourcesPath);
        $manifestFiles = FilesHelper::getManifestFiles($sourcesPath);

        /** @var array<string, MappingSource> $mappingSources */
        $mappingSources = [];

        foreach ($dataFiles as $file) {
            $sourceName = $file->getBasename();
            $mappingSources[$sourceName] = new MappingSource(new LocalFileSource($file));
        }

        foreach ($manifestFiles as $file) {
            $sourceName = $file->getBasename('.manifest');

            if (!isset($mappingSources[$sourceName])) {
                throw new InvalidOutputException(sprintf('Found orphaned table manifest: "%s"', $file->getBasename()));
            }

            $mappingSources[$sourceName]->setManifestFile($file);
        }

        return SliceHelper::sliceSources($this->combineSourcesWithMappingsFromConfiguration(
            $mappingSources,
            $configuration['mapping'] ?? [],
            $isFailedJob,
        ));
    }
}
