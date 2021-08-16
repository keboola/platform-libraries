<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Table\Source\FileSource;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use SplFileInfo;
use Symfony\Component\Finder\Finder;

class LocalTableStrategy extends AbstractTableStrategy
{
    public function resolveMappings($sourcePathPrefix, array $configuration)
    {
        $sourcesPath = $this->metadataStorage->getPath() . '/' . $sourcePathPrefix;
        $dataFiles = ManifestHelper::getNonManifestFiles($sourcesPath);
        $manifestFiles = ManifestHelper::getManifestFiles($sourcesPath);

        /** @var SourceInterface[] $sources */
        $sources = [];

        foreach ($dataFiles as $file) {
            $sources[$file->getBasename()] = new FileSource($sourcePathPrefix, $file);
        }

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

    public function loadDataIntoTable(SourceInterface $source, $tableId, array $options)
    {
        $tags = !empty($options['tags']) ? $options['tags'] : [];
        $sourcePath = $source->getSourceId();

        if (is_dir($sourcePath)) {
            $fileId = $this->uploadSlicedFile($sourcePath, $tags);
        } else {
            $fileId = $this->clientWrapper->getBasicClient()->uploadFile(
                $sourcePath,
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
