<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\DeferredTasks\TableWriterV2\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriterV2\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Temp\Temp;
use LogicException;
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
            $isSliced = $file->isDir();

            $sources[$sourceName] = new MappingSource($sourceName, $sourceId, $isSliced);
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

    public function prepareLoadTask(
        MappingSource $source,
        MappingDestination $destination,
        $destinationTableExists,
        array $config,
        array $loadOptions
    ) {
        $loadOptions = array_merge($loadOptions, [
            'delimiter' => $config['delimiter'],
            'enclosure' => $config['enclosure'],
        ]);

        $tags = !empty($loadOptions['tags']) ? $loadOptions['tags'] : [];
        $hasColumns = !empty($config['columns']);

        if ($source->isSliced() && !$hasColumns) {
            throw new LogicException('Sliced files must have columns configured!');
        }

        if ($source->isSliced()) {
            $loadOptions['dataFileId'] = $this->uploadSlicedFile($source->getId(), $tags);
        } else {
            $loadOptions['dataFileId'] = $this->uploadRegularFile($source->getId(), $tags);
        }

        // some scenarios are not supported by the SAPI, so we need to take care of them manually here
        // - columns in config + headless CSV (SAPI always expect to have a header in CSV)
        // - sliced files
        if (!$destinationTableExists && $hasColumns) {
            $this->createTable($destination, $config['columns'], $loadOptions);
            $destinationTableExists = true;
        }

        if ($destinationTableExists) {
            return new LoadTableTask($destination, $loadOptions);
        }

        return new CreateAndLoadTableTask($destination, $loadOptions);
    }

    private function createTable(MappingDestination $destination, array $columns, array $loadOptions)
    {
        $tmp = new Temp();

        $headerCsvFile = new CsvFile($tmp->createFile($destination->getTableName().'.header.csv'));
        $headerCsvFile->writeRow($columns);

        $this->clientWrapper->getBasicClient()->createTableAsync(
            $destination->getBucketId(),
            $destination->getTableName(),
            $headerCsvFile,
            $loadOptions
        );
    }

    /**
     * @param string $source Slices folder
     * @return string
     * @throws ClientException
     */
    private function uploadSlicedFile($source, array $tags)
    {
        $finder = new Finder();
        $slices = $finder->files()->in($source)->depth(0);
        $sliceFiles = [];
        foreach ($slices as $slice) {
            $sliceFiles[] = $slice->getPathname();
        }

        $fileUploadOptions = (new FileUploadOptions())
            ->setIsSliced(true)
            ->setFileName(basename($source))
            ->setCompress(true)
            ->setTags($tags)
        ;

        return (string) $this->clientWrapper->getBasicClient()->uploadSlicedFile($sliceFiles, $fileUploadOptions);
    }

    /**
     * @param string $source
     * @return string
     * @throws ClientException
     */
    private function uploadRegularFile($source, $tags)
    {
        $fileUploadOptions = (new FileUploadOptions())
            ->setCompress(true)
            ->setTags($tags)
        ;

        return (string) $this->clientWrapper->getBasicClient()->uploadFile($source, $fileUploadOptions);
    }
}
