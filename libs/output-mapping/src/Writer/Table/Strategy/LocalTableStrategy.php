<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Helper\TagsHelper;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\TableWriter;
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

    public function resolveLoadTaskOptions($sourceId, array $config, array $systemMetadata)
    {
        $loadOptions = [
            'delimiter' => $config['delimiter'],
            'enclosure' => $config['enclosure'],
            'columns' => !empty($config['columns']) ? $config['columns'] : [],
            'incremental' => $config['incremental'],
        ];

        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        if (in_array(TableWriter::TAG_STAGING_FILES_FEATURE, $tokenInfo['owner']['features'], true)) {
            $loadOptions = TagsHelper::addSystemTags($loadOptions, $systemMetadata, $this->logger);
        }

        $tags = !empty($loadOptions['tags']) ? $loadOptions['tags'] : [];

        if (is_dir($sourceId)) {
            $loadOptions['dataFileId'] = $this->uploadSlicedFile($sourceId, $tags);
        } else {
            $loadOptions['dataFileId'] = $this->clientWrapper->getBasicClient()->uploadFile(
                $sourceId,
                (new FileUploadOptions())->setCompress(true)->setTags($tags)
            );
        }

        return $loadOptions;
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
