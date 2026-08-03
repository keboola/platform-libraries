<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\Storage\NativeTypeDecisionHelper;
use Keboola\OutputMapping\Storage\TableCreator;
use Keboola\OutputMapping\Storage\TableDescription;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFromColumns;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchema;
use Keboola\OutputMapping\Writer\Table\TableDefinitionInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class LoadTableTaskCreator
{
    private TableCreator $tableCreator;

    public function __construct(readonly ClientWrapper $clientWrapper, readonly LoggerInterface $logger)
    {
        $this->tableCreator = new TableCreator($clientWrapper);
    }

    /**
     * @param TableDescription|null $descriptions descriptions produced by output mapping for the destination
     *     table; they are embedded in the create-table-definition payload whenever the table is created here.
     *     Descriptions of a table which existed before are handled by StoragePreparer, where the
     *     `isDescriptionSystemManaged` flag of the table is known.
     */
    public function create(
        StrategyInterface $strategy,
        MappingFromProcessedConfiguration $source,
        MappingStorageSources $storageSources,
        OutputMappingSettings $settings,
        ?TableDescription $descriptions = null,
    ): LoadTableTaskResult {
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
            $this->createTableDefinition($source, $tableDefinition, $descriptions);
            return new LoadTableTaskResult(new LoadTableTask($source->getDestination(), $loadOptions, true));
        } elseif ($settings->hasNewNativeTypesFeature() &&
            !$storageSources->didTableExistBefore() &&
            $source->getSchema()
        ) {
            $tableDefinition = new TableDefinitionFromSchema(
                $source->getDestination()->getTableName(),
                $source->getSchema(),
                $storageSources->getBucket()->backend,
            );
            $this->createTableDefinition($source, $tableDefinition, $descriptions);
            return new LoadTableTaskResult(new LoadTableTask($source->getDestination(), $loadOptions, true));
        } elseif (!$storageSources->didTableExistBefore() && $source->hasColumns()) {
            // tabulka neexistuje a známe sloupce z manifestu - vytváříme ji přes table definition bez typů
            $tableDefinition = new TableDefinitionFromColumns(
                $source->getDestination()->getTableName(),
                $source->getColumns(),
                $source->getPrimaryKey(),
            );
            $this->createTableDefinition($source, $tableDefinition, $descriptions);
            return new LoadTableTaskResult(new LoadTableTask($source->getDestination(), $loadOptions, true));
        } elseif ($storageSources->didTableExistBefore()) {
            // tabulka existuje takže nahráváme data
            return new LoadTableTaskResult(new LoadTableTask($source->getDestination(), $loadOptions, false));
        } else {
            // tabulka nemá manifest a tím nemá známé columns
            if ($settings->getTreatValuesAsNull() !== null) {
                // @TODO remove after https://keboola.atlassian.net/browse/CT-1858 will be resolved
                $this->logger->warning(sprintf(
                    'Treating values as null for table "%s" was skipped.',
                    $source->getDestination()->getTableName(),
                ));
            }
            // The table is created by the load job itself (Client::queueTableCreate), there is no create
            // payload the descriptions could be embedded into - they have to be stored after the load.
            return new LoadTableTaskResult(
                new CreateAndLoadTableTask($source->getDestination(), $loadOptions, true),
                $descriptions !== null && !$descriptions->isEmpty() ? $descriptions : null,
            );
        }
    }

    private function createTableDefinition(
        MappingFromProcessedConfiguration $source,
        TableDefinitionInterface $tableDefinition,
        ?TableDescription $descriptions,
    ): void {
        // The table is brand new, therefore its description is always system-managed (Storage default) and
        // there is nothing to diff against.
        if ($descriptions !== null && !$descriptions->isEmpty()) {
            $tableDefinition->setDescriptions($descriptions, $this->logger);
        }

        $this->tableCreator->createTableDefinition(
            $source->getDestination()->getBucketId(),
            $tableDefinition,
        );
    }

    public function buildLoadOptions(
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

        if ($source->getDeduplicationStrategy() !== null) {
            $loadOptions['deduplicationStrategy'] = $source->getDeduplicationStrategy();
        }

        return array_merge(
            $loadOptions,
            $strategy->prepareLoadTaskOptions($source),
        );
    }
}
