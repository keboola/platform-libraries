<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\DeferredTasks\LoadTable;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
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
        $dataFiles = FilesHelper::getDataFiles($sourcesPath);
        $manifestFiles = FilesHelper::getManifestFiles($sourcesPath);

        /** @var array<string, MappingSource> $sources */
        $sources = [];

        foreach ($dataFiles as $file) {
            $sourceName = $file->getBasename();
            $sourceId = $file->getPathname();

            $sources[$sourceName] = new MappingSource($sourceName, $sourceId);
        }

        foreach ($manifestFiles as $file) {
            $sourceName = $file->getBasename('.manifest');

            if (!isset($sources[$sourceName])) {
                throw new InvalidOutputException(sprintf('Found orphaned table manifest: "%s"', $file->getBasename()));
            }

            $sources[$sourceName]->setManifestFile($file);
        }

        return $this->combineSourcesWithMappingsFromConfiguration(
            $sources,
            isset($configuration['mapping']) ? $configuration['mapping'] : []
        );
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
