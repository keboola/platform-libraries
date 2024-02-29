<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\TableMetadata;
use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Staging\StagingFactory;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Storage\BucketCreator;
use Keboola\OutputMapping\Storage\TableDataModifier;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\OutputMapping\Storage\TableStructureModifier;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Table\BranchResolver;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\SlicerResolver;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolver;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use Symfony\Component\Finder\SplFileInfo;

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
        $strategy = $this->strategyFactory->getTableOutputStrategy($outputStaging, $isFailedJob);
        $combinedSources = $this->getCombinedSources($strategy, $configuration); // TODO: vykopnout do vlastní metody

        if ($configuration->hasSlicingFeature() && $strategy->hasSlicer()) {
            $sourcesForSlicing = (new SlicerResolver($this->logger))->resolveSliceFiles($combinedSources); // TODO možná SlicerResolver -> SlicerDecider

            $strategy->sliceFiles($sourcesForSlicing);

            $combinedSources = $this->getCombinedSources($strategy, $configuration);
        }

        $loadTableTasks = [];
        $tableConfigurationResolver = new TableConfigurationResolver($this->clientWrapper, $this->logger);
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

            $processedConfig = $tableConfigurationResolver->resolveTableConfiguration(
                $configuration->getDefaultBucket(),
                $combinedSource,
                $configFromManifest,
                $configFromMapping,
                $systemMetadata,
            );


            // TODO include WS check in validator
//            $processedSource = ValidateConfiguration($processedSource);
//            RestrictedColumnsHelper::validateRestrictedColumnsInConfig($processedSource) -> validateTableCOnfiguration

            $processedConfig = (new BranchResolver($this->clientWrapper))->rewriteBranchSource($processedConfig);
            $processedSource = new MappingFromProcessedConfiguration($processedConfig, $combinedSource);

            // If it is a failed job, we only want to upload if the table has write_always = true
             if ($isFailedJob && empty($processedSource->hasWriteAlways())) {
                continue;
            }

            /*
            if ($isDebugJob) { // TODO v původním kódu jsem to nenašel ??
                continue;
            }
            */

            // validateRestrictedColumnsInConfig
            $loadTableTasks[] = $this->createLoadTableTask(
                $strategy,
                $processedSource,
                // RestrictedColumnsHelper::removeRestrictedColumnsFromConfig($config), TODO Move after validateRestrictedColumns
                $systemMetadata,
                false, // #TODO read from RawConfiguration $createTypedTables,
            );

        }

        $tableQueue = new LoadTableQueue($this->clientWrapper, $this->logger, $loadTableTasks);
        $tableQueue->start();
        return $tableQueue;
    }

    private function createLoadTableTask(
        StrategyInterface $strategy,
        //SourceInterface $source,
        MappingFromProcessedConfiguration $source,
        // array $config,
        //array $systemMetadata,
        SystemMetadata $systemMetadata,
        bool $createTypedTables,
    ): LoadTableTaskInterface {
        // move this to validation
        $config = $source->getMapping();
        $hasColumns = !empty($config['columns']);
        $hasColumnsMetadata = !empty($config['column_metadata']);
        if (!$hasColumns && $source->isSliced()) { // TODO vyhodit do validace
            throw new InvalidOutputException(
                sprintf('Sliced file "%s" columns specification missing.', $source->getSourceName()),
            );
        }
        $destination = $source->getDestination();


        try { // TODO vyhodit do validace
            $destination = new MappingDestination($config['destination']);
        } catch (InvalidArgumentException $e) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve valid destination. "%s" is not a valid table ID.',
                $config['destination'],
            ), 0, $e);
        }

        // TODO - to co připravuje Storage tak vyhodit pryč - nesouvisí to s createLoadTableTask
        $bucketCreator = new BucketCreator($this->clientWrapper);
        $destinationBucket = $bucketCreator->ensureDestinationBucket($source->getDestination(), $systemMetadata);
        $destinationTableInfo = $this->getDestinationTableInfoIfExists($destination->getTableId());

        if ($destinationTableInfo !== null) {
            $tableStructureModifier = new TableStructureModifier($this->clientWrapper, $this->logger);
            $tableStructureModifier->updateTableStructure(
                $destinationBucket,
                $destinationTableInfo,
                $source,
                $destination,
            );

            $tableDataModifier = new TableDataModifier($this->clientWrapper);
            $tableDataModifier->updateTableData(
                $source,
                $destination,
            );
        }

        $loadOptions = [
            'columns' => !empty($config['columns']) ? $config['columns'] : [],
            'primaryKey' => implode(',', PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key'])), // TODO není potřeba -> děla se v resolveru
            'incremental' => $config['incremental'],
        ];

        if ($destinationTableInfo === null && isset($config['distribution_key'])) {
            $loadOptions['distributionKey'] = implode(
                ',',
                PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['distribution_key']), // TODO udělat v resolveru jako PKs
            );
        }

        $loadOptions = array_merge(
            $loadOptions,
            $strategy->prepareLoadTaskOptions($source, $config),
        );

        // some scenarios are not supported by the SAPI, so we need to take care of them manually here
        // - columns in config + headless CSV (SAPI always expect to have a header in CSV)
        // - sliced files
        // $destinationTableInfo !== null - tabulka existuje

        if ($createTypedTables && $destinationTableInfo === null && ($hasColumns && $hasColumnsMetadata)) {
            // typovaná tabulka
            $tableDefinitionFactory = new TableDefinitionFactory(
                $config['metadata'] ?? [],
                $destinationBucket['backend'],
            );
            $tableDefinition = $tableDefinitionFactory->createTableDefinition(
                $destination->getTableName(),
                PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']), // TODO už to je dávno hotový
                $config['column_metadata'],
            );
            $this->createTableDefinition($destination, $tableDefinition);
            $tableCreated = true;
            $loadTask = new LoadTableTask($destination, $loadOptions, $tableCreated);
        } elseif ($destinationTableInfo === null && $hasColumns) {
            // tabulka neexistuje a známe sloupce z manifestu
            $this->createTable($destination, $config['columns'], $loadOptions);
            $tableCreated = true;
            $loadTask = new LoadTableTask($destination, $loadOptions, $tableCreated);
        } elseif ($destinationTableInfo !== null) {
            // tabulka existuje takže nahráváme data
            $tableCreated = false;
            $loadTask = new LoadTableTask($destination, $loadOptions, $tableCreated);
        } else {
            // tabulka nemá manifest a tím nemá známé columns
            $tableCreated = true;
            $loadTask = new CreateAndLoadTableTask($destination, $loadOptions, $tableCreated);
        }

        // TODO add metadata vyhodit do svojí metody
        if ($tableCreated) {
            $loadTask->addMetadata(new TableMetadata(
                $destination->getTableId(),
                TableWriter::SYSTEM_METADATA_PROVIDER,
                $systemMetadata->getCreatedMetadata(),
            ));
        }

        $loadTask->addMetadata(new TableMetadata(
            $destination->getTableId(),
            TableWriter::SYSTEM_METADATA_PROVIDER,
            $systemMetadata->getUpdatedMetadata(),
        ));

        if (!empty($config['metadata'])) {
            $loadTask->addMetadata(new TableMetadata(
                $destination->getTableId(),
                $systemMetadata->asArray()[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
                $config['metadata'],
            ));
        }

        if ($hasColumnsMetadata) {
            $loadTask->addMetadata(new ColumnMetadata(
                $destination->getTableId(),
                $systemMetadata->asArray()[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
                $config['column_metadata'],
            ));
        }

        return $loadTask;
    }

    private function getDestinationTableInfoIfExists(string $tableId): ?TableInfo
    {
        try {
            return new TableInfo($this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId));
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        return null;
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
    private function getCombinedSources(StrategyInterface $strategy, RawConfiguration $configuration): array
    {
        $stagingFactory = new StagingFactory($strategy->getDataStorage()->getPath());

        $physicalDataFiles = $strategy->listSources($configuration->getSourcePathPrefix(), $configuration->getMapping());
        $physicalManifests = $strategy->listManifests($configuration->getSourcePathPrefix());

        $sourcesValidator = $stagingFactory->getSourcesValidator();
        $sourcesValidator->validate($physicalDataFiles, $physicalManifests);

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
