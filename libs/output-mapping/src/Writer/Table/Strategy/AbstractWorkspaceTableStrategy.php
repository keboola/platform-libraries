<?php

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\DeferredTasks\TableWriterV2\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriterV2\LoadTableTask;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\Temp\Temp;
use LogicException;
use Symfony\Component\Finder\SplFileInfo;

abstract class AbstractWorkspaceTableStrategy extends AbstractTableStrategy
{
    public function resolveMappingSources($sourcePathPrefix, array $configuration)
    {
        $sourcesPath = Path::join($this->metadataStorage->getPath(), $sourcePathPrefix);
        $manifestFiles = FilesHelper::getManifestFiles($sourcesPath);

        /** @var array<string, MappingSource> $sources */
        $sources = [];

        foreach (isset($configuration['mapping']) ? $configuration['mapping'] : [] as $mapping) {
            $sourceName = $mapping['source'];

            // Create empty mapping. This is workaround for not being able to list real list of tables from workspace.
            $sources[$sourceName] = $this->createMapping($sourcePathPrefix, $sourceName, null, null);
        }

        foreach ($manifestFiles as $file) {
            $sourceName = $file->getBasename('.manifest');

            if (isset($sources[$sourceName])) {
                $sources[$sourceName]->setManifestFile($file);
            } else {
                $sources[$sourceName] = $this->createMapping($sourcePathPrefix, $sourceName, $file, null);
            }
        }

        return $this->combineSourcesWithMappingsFromConfiguration(
            $sources,
            isset($configuration['mapping']) ? $configuration['mapping'] : []
        );
    }

    /**
     * @param string $sourcePathPrefix
     * @param string $sourceName
     * @param null|SplFileInfo $manifestFile
     * @param null|array $mapping
     * @return MappingSource
     */
    abstract protected function createMapping($sourcePathPrefix, $sourceName, $manifestFile, $mapping);

    public function prepareLoadTask(
        MappingSource $source,
        MappingDestination $destination,
        $destinationTableExists,
        array $config,
        array $loadOptions
    ) {
        $loadOptions = array_merge($loadOptions, [
            'dataWorkspaceId' => $this->dataStorage->getWorkspaceId(),
            'dataObject' => $source->getId(),
        ]);

        $hasColumns = !empty($config['columns']);

        if ($source->isSliced() && !$hasColumns) {
            throw new LogicException('Sliced files must have columns configured!');
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
}
