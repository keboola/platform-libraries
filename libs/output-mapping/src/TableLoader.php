<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\Staging\StagingFactory;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\Table\BranchResolver;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\MetadataSetter;
use Keboola\OutputMapping\Writer\Table\SlicerDecider;
use Keboola\OutputMapping\Storage\StoragePreparer;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolver;
use Keboola\OutputMapping\Writer\Table\TableConfigurationValidator;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Throwable;

class TableLoader
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly ClientWrapper $clientWrapper,
        private readonly StrategyFactory $strategyFactory,
    ) {
    }

    public function uploadTables(
        string $outputStaging,
        RawConfiguration $configuration,
        SystemMetadata $systemMetadata,
        bool $isFailedJob,
    ): LoadTableQueue {
        if (empty($systemMetadata->asArray()[AbstractWriter::SYSTEM_KEY_COMPONENT_ID])) {
            throw new OutputOperationException('Component Id must be set');
        }

        $strategy = $this->strategyFactory->getTableOutputStrategy($outputStaging, $isFailedJob);
        $combinedSources = $this->getCombinedSources($strategy, $configuration, $isFailedJob);

        if ($configuration->hasSlicingFeature() && $strategy->hasSlicer()) {
            $sourcesForSlicing = (new SlicerDecider($this->logger))->decideSliceFiles($combinedSources);

            $strategy->sliceFiles($sourcesForSlicing);

            $combinedSources = $this->getCombinedSources($strategy, $configuration, $isFailedJob);
        }

        $loadTableTasks = [];
        $tableConfigurationResolver = new TableConfigurationResolver($this->logger);
        $tableConfigurationValidator = new TableConfigurationValidator();
        foreach ($combinedSources as $combinedSource) {
            $this->logger->info(sprintf('Loading table "%s"', $combinedSource->getSourceName()));
            $configFromManifest = $strategy->readFileManifest(sprintf(
                '%s/%s.manifest',
                $configuration->getSourcePathPrefix(),
                $combinedSource->getSourceName(),
            ));

            $configFromMapping = $combinedSource->getConfiguration() ?
                $combinedSource->getConfiguration()->asArray() :
                [];
            unset($configFromMapping['source']); // TODO - zjistit proč se to unsetuje

            try {
                $processedConfig = $tableConfigurationResolver->resolveTableConfiguration(
                    $configuration,
                    $combinedSource,
                    $configFromManifest,
                    $configFromMapping,
                    $systemMetadata,
                );

                $processedConfig = (new BranchResolver($this->clientWrapper))->rewriteBranchSource($processedConfig);
                $processedConfig = $tableConfigurationValidator->validate($strategy, $combinedSource, $processedConfig);
            } catch (Throwable $e) {
                if (!$isFailedJob) {
                    throw $e;
                }
            }

            // If it is a failed job, we only want to upload if the table has write_always = true
             if ($isFailedJob && empty($processedConfig['write_always'])) {
                continue;
            }

            $processedSource = new MappingFromProcessedConfiguration($processedConfig, $combinedSource);

            $storagePreparer = new StoragePreparer($this->clientWrapper, $this->logger);
            $storageSources = $storagePreparer->prepareStorageBucketAndTable($processedSource, $systemMetadata);

            $loadTableTask = $this->createLoadTableTask(
                $strategy,
                $processedSource,
                $storageSources,
                $configuration->isCreateTypedTables(),
            );

            $metadataSetter = new MetadataSetter();
            $metadataSetter->setTableMetadata($loadTableTask, $processedSource, $storageSources, $systemMetadata);

            $loadTableTasks[] = $loadTableTask;
        }

        $tableQueue = new LoadTableQueue($this->clientWrapper, $this->logger, $loadTableTasks);
        $tableQueue->start();
        return $tableQueue;
    }

    private function createLoadTableTask(
        StrategyInterface $strategy,
        MappingFromProcessedConfiguration $source,
        MappingStorageSources $storageSources,
        bool $createTypedTables,
    ): LoadTableTaskInterface {
        $loadOptions = [
            'columns' => $source->getColumns(),
            'primaryKey' => implode(',', $source->getPrimaryKey()),
            'incremental' => $source->isIncremental(),
        ];

        if (!$storageSources->hasTable() && $source->hasDistributionKey()) {
            $loadOptions['distributionKey'] = implode(',', $source->getDistributionKey());
        }

        $loadOptions = array_merge(
            $loadOptions,
            $strategy->prepareLoadTaskOptions($source),
        );

        // some scenarios are not supported by the SAPI, so we need to take care of them manually here
        // - columns in config + headless CSV (SAPI always expect to have a header in CSV)
        // - sliced files
        if ($createTypedTables && !$storageSources->hasTable() && ($source->hasColumns() && $source->hasColumnMetadata())) {
            // typovaná tabulka
            $tableDefinitionFactory = new TableDefinitionFactory(
                $source->hasMetadata() ? $source->getMetadata() : [],
                $storageSources->getBucket()->backend,
            );
            $tableDefinition = $tableDefinitionFactory->createTableDefinition(
                $source->getDestination()->getTableName(),
                $source->getPrimaryKey(),
                $source->getColumnMetadata(),
            );
            $this->createTableDefinition($source->getDestination(), $tableDefinition);
            $tableCreated = true;
            $loadTask = new LoadTableTask($source->getDestination(), $loadOptions, $tableCreated);
        } elseif (!$storageSources->hasTable() && $source->hasColumns()) {
            // tabulka neexistuje a známe sloupce z manifestu
            $this->createTable($source->getDestination(), $source->getColumns(), $loadOptions);
            $tableCreated = true;
            $loadTask = new LoadTableTask($source->getDestination(), $loadOptions, $tableCreated);
        } elseif ($storageSources->hasTable()) {
            // tabulka existuje takže nahráváme data
            $tableCreated = false;
            $loadTask = new LoadTableTask($source->getDestination(), $loadOptions, $tableCreated);
        } else {
            // tabulka nemá manifest a tím nemá známé columns
            $tableCreated = true;
            $loadTask = new CreateAndLoadTableTask($source->getDestination(), $loadOptions, $tableCreated);
        }
        return $loadTask;
    }

    private function createTable(MappingDestination $destination, array $columns, array $loadOptions): void
    {
        $tmp = new Temp();

        $headerCsvFile = new CsvFile($tmp->createFile($destination->getTableName().'.header.csv')->getPathname());
        $headerCsvFile->writeRow($columns);

        $this->clientWrapper->getTableAndFileStorageClient()->createTableAsync(
            $destination->getBucketId(),
            $destination->getTableName(),
            $headerCsvFile,
            $loadOptions,
        );
    }

    private function createTableDefinition(MappingDestination $destination, TableDefinition $tableDefinition): void
    {
        $requestData = $tableDefinition->getRequestData();

        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition(
                $destination->getBucketId(),
                $requestData,
            );
        } catch (ClientException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Cannot create table "%s" definition in Storage API: %s',
                    $destination->getTableName(),
                    json_encode((array) $e->getContextParams()),
                ),
                $e->getCode(),
                $e,
            );
        }
    }

    /**
     * @return MappingFromRawConfigurationAndPhysicalDataWithManifest[]
     */
    private function getCombinedSources(StrategyInterface $strategy, RawConfiguration $configuration, bool $isFailedJob): array
    {
        $stagingFactory = new StagingFactory($strategy->getDataStorage()->getPath());
        $sourcesValidator = $stagingFactory->getSourcesValidator();

        $physicalDataFiles = $strategy->listSources($configuration->getSourcePathPrefix(), $configuration->getMapping());
        $physicalManifests = $strategy->listManifests($configuration->getSourcePathPrefix());

        $sourcesValidator->validatePhysicalFilesWithManifest($physicalDataFiles, $physicalManifests);
        $sourcesValidator->validatePhysicalFilesWithConfiguration($physicalDataFiles, $configuration->getMapping(), $isFailedJob);

        $mappingCombiner = new Mapping\MappingCombiner();
        $combinedSources = $mappingCombiner->combineDataItemsWithConfigurations(
            $physicalDataFiles,
            $configuration->getMapping(),
        );


        $physicalManifests = $strategy->listManifests($configuration->getSourcePathPrefix());
        return $mappingCombiner->combineSourcesWithManifests(
            $combinedSources,
            $physicalManifests,
        );
    }

}
