<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\Configuration\Table\Webalizer;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Exception\TableNotFoundException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Storage\StoragePreparer;
use Keboola\OutputMapping\Storage\TableChangesStore;
use Keboola\OutputMapping\Storage\TableStructureValidatorFactory;
use Keboola\OutputMapping\Writer\Table\BranchResolver;
use Keboola\OutputMapping\Writer\Table\MetadataSetter;
use Keboola\OutputMapping\Writer\Table\SlicerDecider;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolver;
use Keboola\OutputMapping\Writer\Table\TableConfigurationValidator;
use Keboola\OutputMapping\Writer\Table\TableHintsConfigurationSchemaResolver;
use Keboola\StorageApiBranch\ClientWrapper;
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

            $loadTableTaskCreator = new LoadTableTaskCreator($this->clientWrapper, $this->logger);
            $loadTableTask = $loadTableTaskCreator->create(
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
}
