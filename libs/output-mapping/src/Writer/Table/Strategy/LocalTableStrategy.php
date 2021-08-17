<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use SplFileInfo;
use Symfony\Component\Finder\Finder;

class LocalTableStrategy extends AbstractTableStrategy
{
    public function resolveMappingSources($sourcePathPrefix, array $configuration)
    {
        $sourcesPath = Path::join($this->metadataStorage->getPath(), $sourcePathPrefix);
        $dataFiles = ManifestHelper::getNonManifestFiles($sourcesPath);
        $manifestFiles = ManifestHelper::getManifestFiles($sourcesPath);

        /** @var array<string, MappingSource> $sources */
        $sources = [];

        foreach ($dataFiles as $file) {
            $filename = $file->getBasename();
            $pathname = $file->getPathname();

            $sources[$file->getBasename()] = new MappingSource($filename, $pathname);
        }

        foreach ($manifestFiles as $file) {
            $dataFileName = $file->getBasename('.manifest');

            if (!isset($sources[$dataFileName])) {
                throw new InvalidOutputException(sprintf('Found orphaned table manifest: "%s"', $file->getBasename()));
            }

            $sources[$dataFileName]->setManifestFile($file);
        }

        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $filename = $mapping['source'];

                if (!isset($sources[$filename])) {
                    throw new InvalidOutputException(sprintf('Table source "%s" not found.', $mapping['source']), 404);
                }
            }

            $sources = $this->combineSourcesWithCustomMappings($sources, $configuration['mapping']);
        }

        return array_values($sources);
    }

    /**
     * @param MappingSource[] $sources
     * @param array<array{source: string}> $mappings
     * @return MappingSource[]
     */
    private function combineSourcesWithCustomMappings(array $sources, array $mappings)
    {
        $mappingsBySource = [];
        foreach ($mappings as $mapping) {
            $mappingsBySource[$mapping['source']][] = $mapping;
        }

        $sourcesWithMapping = [];
        foreach ($sources as $source) {
            $sourceMappings = isset($mappingsBySource[$source->getName()]) ?
                $mappingsBySource[$source->getName()] :
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

    public function loadDataIntoTable($sourceId, $tableId, array $options)
    {
        $tags = !empty($options['tags']) ? $options['tags'] : [];

        if (is_dir($sourceId)) {
            $fileId = $this->uploadSlicedFile($sourceId, $tags);
        } else {
            $fileId = $this->clientWrapper->getBasicClient()->uploadFile(
                $sourceId,
                (new FileUploadOptions())->setCompress(true)->setTags($tags)
            );
        }

        $options['dataFileId'] = $fileId;
        return new LoadTable($this->clientWrapper->getBasicClient(), $tableId, $options);
    }

    /**
     * Uploads a sliced table to storage api. Takes all files from the $source folder
     *
     * @param string $source Slices folder
     * @return string
     * @throws ClientException
     */
    private function uploadSlicedFile($source, $tags)
    {
        $finder = new Finder();
        $slices = $finder->files()->in($source)->depth(0);
        $sliceFiles = [];
        /** @var SplFileInfo $slice */
        foreach ($slices as $slice) {
            $sliceFiles[] = $slice->getPathname();
        }

        // upload slices
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName(basename($source))
            ->setCompress(true)
            ->setTags($tags);
        return $this->clientWrapper->getBasicClient()->uploadSlicedFile($sliceFiles, $fileUploadOptions);
    }
}
