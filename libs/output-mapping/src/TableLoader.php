<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\Configuration\Table\Webalizer;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Exception\TableNotFoundException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Storage\NativeTypeDecisionHelper;
use Keboola\OutputMapping\Storage\StoragePreparer;
use Keboola\OutputMapping\Storage\TableChangesStore;
use Keboola\OutputMapping\Storage\TableCreator;
use Keboola\OutputMapping\Storage\TableStructureValidatorFactory;
use Keboola\OutputMapping\Writer\Table\BranchResolver;
use Keboola\OutputMapping\Writer\Table\MetadataSetter;
use Keboola\OutputMapping\Writer\Table\SlicerDecider;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolver;
use Keboola\OutputMapping\Writer\Table\TableConfigurationValidator;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchema;
use Keboola\OutputMapping\Writer\Table\TableHintsConfigurationSchemaResolver;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Throwable;

class TableLoader
{
    private readonly TableCreator $tableCreator;

    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly ClientWrapper $clientWrapper,
        private readonly StrategyFactory $strategyFactory,
    ) {
        $this->tableCreator = new TableCreator($this->clientWrapper);
    }

    public function uploadTables(
        string $outputStaging,
        OutputMappingSettings $configuration,
        SystemMetadata $systemMetadata,
    ): LoadTableQueue {
        $strategy = $this->strategyFactory->getTableOutputStrategy($outputStaging, $configuration->isFailedJob());
        $combinedSources = $this->getCombinedSources($strategy, $configuration);

        if ($configuration->hasSlicingFeature() && $strategy->hasSlicer()) {
            $sourcesForSlicing = (new SlicerDecider($this->logger))->decideSliceFiles($combinedSources);

            $strategy->sliceFiles($sourcesForSlicing, $configuration->getDataTypeSupport());

            $combinedSources = $this->getCombinedSources($strategy, $configuration);
        }

        $loadTableTasks = [];
        $tableConfigurationResolver = new TableConfigurationResolver($this->logger);
        $tableConfigurationValidator = new TableConfigurationValidator($strategy, $configuration);
        $tableColumnsConfigurationHintsResolver = new TableHintsConfigurationSchemaResolver();
        $configWebalizer = new Webalizer(
            $this->clientWrapper->getTableAndFileStorageClient(),
            $this->logger,
            $configuration->hasConnectionWebalizeFeature(),
        );

        foreach ($combinedSources as $combinedSource) {
            $this->logger->info(sprintf('Loading table "%s"', $combinedSource->getSourceName()));

            $configFromMapping = $combinedSource->getConfiguration() ?
                $combinedSource->getConfiguration()->asArray() :
                [];

            $configFromManifest = $combinedSource->getManifest() ?
                $strategy->readFileManifest($combinedSource->getManifest()) : [];

            $configFromMapping = $configWebalizer->webalize($configFromMapping);
            $configFromManifest = $configWebalizer->webalize($configFromManifest);

            try {
                $processedConfig = [];
                $processedConfig = $tableConfigurationResolver->resolveTableConfiguration(
                    $configuration,
                    $combinedSource,
                    $configFromManifest,
                    $configFromMapping,
                    $systemMetadata,
                );

                $processedConfig = (new BranchResolver($this->clientWrapper))->rewriteBranchSource($processedConfig);
                if ($configuration->getDataTypeSupport() === OutputMappingSettings::DATA_TYPES_SUPPORT_HINTS) {
                    $processedConfig = $tableColumnsConfigurationHintsResolver
                        ->resolveColumnsConfiguration($processedConfig);
                }

                $tableConfigurationValidator->validate($combinedSource, $processedConfig);
            } catch (Throwable $e) {
                if (!$configuration->isFailedJob()) {
                    throw $e;
                }
            }

            // If it is a failed job, we only want to upload if the table has write_always = true
            if ($configuration->isFailedJob() && empty($processedConfig['write_always'])) {
                continue;
            }

            $processedSource = new MappingFromProcessedConfiguration($processedConfig, $combinedSource);

            try {
                $factory = new TableStructureValidatorFactory($this->logger, $this->clientWrapper);
                $tableStructureValidator = $factory->ensureStructureValidator(
                    $processedSource->getDestination()->getTableId(),
                );

                $tableChangesStore = $tableStructureValidator->validate($processedSource->getSchema());
            } catch (TableNotFoundException) {
                $tableChangesStore = new TableChangesStore();
                // table not found, we will create it
            } catch (InvalidTableStructureException $e) {
                throw new InvalidOutputException($e->getMessage(), $e->getCode(), $e);
            }

            $storagePreparer = new StoragePreparer(
                $this->clientWrapper,
                $this->logger,
                $configuration->hasNewNativeTypesFeature(),
                $configuration->hasBigqueryNativeTypesFeature(),
            );
            $storageSources = $storagePreparer->prepareStorageBucketAndTable(
                $processedSource,
                $systemMetadata,
                $tableChangesStore,
            );

            $loadTableTask = $this->createLoadTableTask(
                $strategy,
                $processedSource,
                $storageSources,
                $configuration,
            );

            $metadataSetter = new MetadataSetter();
            $loadTableTask = $metadataSetter->setTableMetadata(
                $loadTableTask,
                $processedSource,
                $storageSources,
                $systemMetadata,
            );

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
        OutputMappingSettings $settings,
    ): LoadTableTaskInterface {
        $loadOptions = $this->buildLoadOptions(
            $source,
            $strategy,
            $storageSources,
            $settings->hasNewNativeTypesFeature(),
            $settings->getTreatValuesAsNull(),
        );

        // some scenarios are not supported by the SAPI, so we need to take care of them manually here
        // - columns in config + headless CSV (SAPI always expect to have a header in CSV)
        // - sliced files
        if ($settings->hasNativeTypesFeature() &&
            !$storageSources->didTableExistBefore() &&
            $source->hasColumns() && $source->hasColumnMetadata()
        ) {
            // typovaná tabulka
            $backend = $storageSources->getBucket()->backend;

            $tableDefinitionFactory = new TableDefinitionFactory(
                $source->hasMetadata() ? $source->getMetadata() : [],
                $backend,
                NativeTypeDecisionHelper::shouldEnforceBaseTypes(
                    $settings->hasBigqueryNativeTypesFeature(),
                    $backend,
                ),
            );

            $tableDefinition = $tableDefinitionFactory->createTableDefinition(
                $source->getDestination()->getTableName(),
                $source->getPrimaryKey(),
                $source->getColumnMetadata(),
            );
            $this->tableCreator->createTableDefinition($source->getDestination()->getBucketId(), $tableDefinition);
            $loadTask = new LoadTableTask($source->getDestination(), $loadOptions, true);
        } elseif ($settings->hasNewNativeTypesFeature() &&
            !$storageSources->didTableExistBefore() &&
            $source->getSchema()
        ) {
            $tableDefinition = new TableDefinitionFromSchema(
                $source->getDestination()->getTableName(),
                $source->getSchema(),
                $storageSources->getBucket()->backend,
            );
            $this->tableCreator->createTableDefinition($source->getDestination()->getBucketId(), $tableDefinition);
            $loadTask = new LoadTableTask($source->getDestination(), $loadOptions, true);
        } elseif (!$storageSources->didTableExistBefore() && $source->hasColumns()) {
            // tabulka neexistuje a známe sloupce z manifestu
            $this->tableCreator->createTable(
                $source->getDestination()->getBucketId(),
                $source->getDestination()->getTableName(),
                $source->getColumns(),
                $loadOptions,
            );
            $loadTask = new LoadTableTask($source->getDestination(), $loadOptions, true);
        } elseif ($storageSources->didTableExistBefore()) {
            // tabulka existuje takže nahráváme data
            $loadTask = new LoadTableTask($source->getDestination(), $loadOptions, false);
        } else {
            // tabulka nemá manifest a tím nemá známé columns
            if ($settings->getTreatValuesAsNull() !== null) {
                // @TODO remove after https://keboola.atlassian.net/browse/CT-1858 will be resolved
                $this->logger->warning(sprintf(
                    'Treating values as null for table "%s" was skipped.',
                    $source->getDestination()->getTableName(),
                ));
            }
            $loadTask = new CreateAndLoadTableTask($source->getDestination(), $loadOptions, true);
        }
        return $loadTask;
    }

    /**
     * @return MappingFromRawConfigurationAndPhysicalDataWithManifest[]
     */
    private function getCombinedSources(StrategyInterface $strategy, OutputMappingSettings $configuration): array
    {
        $sourcesValidator = $strategy->getSourcesValidator();

        $physicalDataFiles = $strategy->listSources(
            $configuration->getSourcePathPrefix(),
            $configuration->getMapping(),
        );
        $physicalManifests = $strategy->listManifests($configuration->getSourcePathPrefix());

        $sourcesValidator->validatePhysicalFilesWithManifest($physicalDataFiles, $physicalManifests);
        $sourcesValidator->validatePhysicalFilesWithConfiguration($physicalDataFiles, $configuration->getMapping());
        $sourcesValidator->validateManifestWithConfiguration($physicalManifests, $configuration->getMapping());

        $mappingCombiner = $strategy->getMappingCombiner();
        $combinedSources = $mappingCombiner->combineDataItemsWithConfigurations(
            $physicalDataFiles,
            $configuration->getMapping(),
        );

        return $mappingCombiner->combineSourcesWithManifests(
            $combinedSources,
            $physicalManifests,
        );
    }

    private function buildLoadOptions(
        MappingFromProcessedConfiguration $source,
        StrategyInterface $strategy,
        MappingStorageSources $storageSources,
        bool $hasNewNativeTypesFeature,
        ?array $treatValuesAsNullConfiguration,
    ): array {
        if ($hasNewNativeTypesFeature && $source->getSchema()) {
            $columns = array_map(
                fn (MappingFromConfigurationSchemaColumn $column) => $column->getName(),
                $source->getSchema(),
            );
            $primaryKeys = array_map(
                fn (MappingFromConfigurationSchemaColumn $column) => $column->getName(),
                array_filter(
                    $source->getSchema(),
                    fn (MappingFromConfigurationSchemaColumn $column) => $column->isPrimaryKey(),
                ),
            );
            $distributionKey = array_map(
                fn (MappingFromConfigurationSchemaColumn $column) => $column->getName(),
                array_filter(
                    $source->getSchema(),
                    fn (MappingFromConfigurationSchemaColumn $column) => $column->isDistributionKey(),
                ),
            );
        } else {
            $columns = $source->getColumns();
            $primaryKeys = $source->getPrimaryKey();
            $distributionKey = $source->getDistributionKey();
        }

        $loadOptions = [
            'columns' => $columns,
            'primaryKey' => implode(',', $primaryKeys),
            'incremental' => $source->isIncremental(),
        ];

        if ($source->hasHeader()) {
            $loadOptions['ignoredLinesCount'] = 1;
        }

        if (!$storageSources->didTableExistBefore() && $distributionKey) {
            $loadOptions['distributionKey'] = implode(',', $distributionKey);
        }

        if ($treatValuesAsNullConfiguration !== null) {
            $loadOptions['treatValuesAsNull'] = $treatValuesAsNullConfiguration;
        }

        return array_merge(
            $loadOptions,
            $strategy->prepareLoadTaskOptions($source),
        );
    }
}
