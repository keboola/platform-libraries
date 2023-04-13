<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer;

use InvalidArgumentException;
use Keboola\Csv\CsvFile;
use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\TableMetadata;
use Keboola\OutputMapping\DeferredTasks\TableWriter\CreateAndLoadTableTask;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\OutputMapping\Writer\Helper\TableColumnsHelper;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\StrategyInterface;
use Keboola\OutputMapping\Writer\Table\TableConfigurationResolver;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinition;
use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\Temp\Temp;
use Throwable;

class TableWriter extends AbstractWriter
{
    public const SYSTEM_METADATA_PROVIDER = 'system';
    public const KBC_LAST_UPDATED_BY_BRANCH_ID = 'KBC.lastUpdatedBy.branch.id';
    public const KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id';
    public const KBC_LAST_UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id';
    public const KBC_LAST_UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id';
    public const KBC_CREATED_BY_BRANCH_ID = 'KBC.createdBy.branch.id';
    public const KBC_CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id';
    public const KBC_CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id';
    public const KBC_CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id';
    public const TAG_STAGING_FILES_FEATURE = 'tag-staging-files';

    private Metadata $metadataClient;

    private TableConfigurationResolver $tableConfigurationResolver;

    public function __construct(StrategyFactory $strategyFactory)
    {
        parent::__construct($strategyFactory);

        $this->metadataClient = new Metadata($this->clientWrapper->getBasicClient());
        $this->tableConfigurationResolver = new TableConfigurationResolver(
            $strategyFactory->getClientWrapper(),
            $strategyFactory->getLogger()
        );
        $this->logger = $strategyFactory->getLogger();
    }

    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function setFormat(string $format): void
    {
        $this->tableConfigurationResolver->setFormat($format);
    }

    public function uploadTables(
        string $sourcePathPrefix,
        array $configuration,
        array $systemMetadata,
        string $stagingStorageOutput,
        bool $createTypedTables,
        bool $isFailedJob
    ): LoadTableQueue {
        if (empty($systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID])) {
            throw new OutputOperationException('Component Id must be set');
        }

        $strategy = $this->strategyFactory->getTableOutputStrategy($stagingStorageOutput, $isFailedJob);
        $mappingSources = $strategy->resolveMappingSources($sourcePathPrefix, $configuration);

        $defaultBucket = $configuration['bucket'] ?? null;
        $loadTableTasks = [];
        foreach ($mappingSources as $mappingSource) {
            $config = [];
            try {
                $config = $this->tableConfigurationResolver->resolveTableConfiguration(
                    $mappingSource,
                    $defaultBucket,
                    $systemMetadata
                );
            } catch (Throwable $e) {
                if (!$isFailedJob) {
                    throw $e;
                }
            }

            // If it is a failed job, we only want to upload if the table has write_always = true
            if ($isFailedJob && empty($config['write_always'])) {
                continue;
            }

            try {
                $loadTableTasks[] = $this->createLoadTableTask(
                    $strategy,
                    $mappingSource->getSource(),
                    $config,
                    $systemMetadata,
                    $createTypedTables
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Cannot upload file "%s" to table "%s" in Storage API: %s',
                        $mappingSource->getSourceName(),
                        $config['destination'],
                        $e->getMessage()
                    ),
                    $e->getCode(),
                    $e
                );
            }
        }

        $tableQueue = new LoadTableQueue($this->clientWrapper->getBasicClient(), $this->logger, $loadTableTasks);
        $tableQueue->start();
        return $tableQueue;
    }

    private function getDestinationTableInfoIfExists(string $tableId, Client $storageApiClient): ?array
    {
        try {
            return $storageApiClient->getTable($tableId);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }

        return null;
    }

    private function createLoadTableTask(
        StrategyInterface $strategy,
        SourceInterface $source,
        array $config,
        array $systemMetadata,
        bool $createTypedTables
    ): LoadTableTaskInterface {
        $hasColumns = !empty($config['columns']);
        $hasColumnsMetadata = !empty($config['column_metadata']);
        if (!$hasColumns && $source->isSliced()) {
            throw new InvalidOutputException(
                sprintf('Sliced file "%s" columns specification missing.', $source->getName())
            );
        }

        try {
            $destination = new MappingDestination($config['destination']);
        } catch (InvalidArgumentException $e) {
            throw new InvalidOutputException(sprintf(
                'Failed to resolve valid destination. "%s" is not a valid table ID.',
                $config['destination']
            ), 0, $e);
        }

        $destinationBucket = $this->ensureDestinationBucket($destination, $systemMetadata);
        $storageApiClient = $this->clientWrapper->getBasicClient();
        $destinationTableInfo = $this->getDestinationTableInfoIfExists($destination->getTableId(), $storageApiClient);

        if ($destinationTableInfo !== null) {
            TableColumnsHelper::addMissingColumns(
                $storageApiClient,
                $destinationTableInfo,
                $config,
                $destinationBucket['backend']
            );

            if (PrimaryKeyHelper::modifyPrimaryKeyDecider($this->logger, $destinationTableInfo, $config)) {
                PrimaryKeyHelper::modifyPrimaryKey(
                    $this->logger,
                    $storageApiClient,
                    $destination->getTableId(),
                    $destinationTableInfo['primaryKey'],
                    $config['primary_key']
                );
            }

            if (!empty($config['delete_where_column'])) {
                // Delete rows
                $deleteOptions = [
                    'whereColumn' => $config['delete_where_column'],
                    'whereOperator' => $config['delete_where_operator'],
                    'whereValues' => $config['delete_where_values'],
                ];
                $storageApiClient->deleteTableRows($destination->getTableId(), $deleteOptions);
            }
        }

        $loadOptions = [
            'columns' => !empty($config['columns']) ? $config['columns'] : [],
            'primaryKey' => implode(',', PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key'])),
            'incremental' => $config['incremental'],
        ];

        if ($destinationTableInfo === null && isset($config['distribution_key'])) {
            $loadOptions['distributionKey'] = implode(
                ',',
                PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['distribution_key'])
            );
        }

        $loadOptions = array_merge(
            $loadOptions,
            $strategy->prepareLoadTaskOptions($source, $config)
        );

        // some scenarios are not supported by the SAPI, so we need to take care of them manually here
        // - columns in config + headless CSV (SAPI always expect to have a header in CSV)
        // - sliced files
        if ($createTypedTables && $destinationTableInfo === null && ($hasColumns && $hasColumnsMetadata)) {
            $tableDefinitionFactory = new TableDefinitionFactory(
                $config['metadata'] ?? [],
                $destinationBucket['backend']
            );
            $tableDefinition = $tableDefinitionFactory->createTableDefinition(
                $destination->getTableName(),
                PrimaryKeyHelper::normalizeKeyArray($this->logger, $config['primary_key']),
                $config['column_metadata']
            );
            $this->createTableDefinition($destination, $tableDefinition);
            $tableCreated = true;
            $loadTask = new LoadTableTask($destination, $loadOptions, $tableCreated);
        } elseif ($destinationTableInfo === null && $hasColumns) {
            $this->createTable($destination, $config['columns'], $loadOptions);
            $tableCreated = true;
            $loadTask = new LoadTableTask($destination, $loadOptions, $tableCreated);
        } elseif ($destinationTableInfo !== null) {
            $tableCreated = false;
            $loadTask = new LoadTableTask($destination, $loadOptions, $tableCreated);
        } else {
            $tableCreated = true;
            $loadTask = new CreateAndLoadTableTask($destination, $loadOptions, $tableCreated);
        }

        if ($tableCreated) {
            $loadTask->addMetadata(new TableMetadata(
                $destination->getTableId(),
                TableWriter::SYSTEM_METADATA_PROVIDER,
                $this->getCreatedMetadata($systemMetadata)
            ));
        }

        $loadTask->addMetadata(new TableMetadata(
            $destination->getTableId(),
            TableWriter::SYSTEM_METADATA_PROVIDER,
            $this->getUpdatedMetadata($systemMetadata)
        ));

        if (!empty($config['metadata'])) {
            $loadTask->addMetadata(new TableMetadata(
                $destination->getTableId(),
                $systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
                $config['metadata']
            ));
        }

        if ($hasColumnsMetadata) {
            $loadTask->addMetadata(new ColumnMetadata(
                $destination->getTableId(),
                $systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
                $config['column_metadata']
            ));
        }

        return $loadTask;
    }

    /**
     * @param MappingDestination $destination
     * @param array $systemMetadata
     * @return array{id: string, backend: string}
     * @throws ClientException
     */
    private function ensureDestinationBucket(MappingDestination $destination, array $systemMetadata): array
    {
        $destinationBucketId = $destination->getBucketId();
        try {
            $destinationBucketDetails = $this->clientWrapper->getBasicClient()->getBucket($destinationBucketId);
            $this->checkDevBucketMetadata($destination);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
            // bucket doesn't exist so we need to create it
            $this->createDestinationBucket($destination, $systemMetadata);
            $destinationBucketDetails = $this->clientWrapper->getBasicClient()->getBucket($destinationBucketId);
        }

        return [
            'id' => $destinationBucketDetails['id'],
            'backend' => $destinationBucketDetails['backend'],
        ];
    }

    private function createDestinationBucket(MappingDestination $destination, array $systemMetadata): void
    {
        $this->clientWrapper->getBasicClient()->createBucket(
            $destination->getBucketName(),
            $destination->getBucketStage()
        );

        $this->metadataClient->postBucketMetadata(
            $destination->getBucketId(),
            TableWriter::SYSTEM_METADATA_PROVIDER,
            $this->getCreatedMetadata($systemMetadata)
        );
    }

    private function createTable(MappingDestination $destination, array $columns, array $loadOptions): void
    {
        $tmp = new Temp();

        $headerCsvFile = new CsvFile($tmp->createFile($destination->getTableName().'.header.csv')->getPathname());
        $headerCsvFile->writeRow($columns);

        $this->clientWrapper->getBasicClient()->createTableAsync(
            $destination->getBucketId(),
            $destination->getTableName(),
            $headerCsvFile,
            $loadOptions
        );
    }

    private function createTableDefinition(MappingDestination $destination, TableDefinition $tableDefinition): void
    {
        $requestData = $tableDefinition->getRequestData();

        try {
            $this->clientWrapper->getBasicClient()->createTableDefinition(
                $destination->getBucketId(),
                $requestData
            );
        } catch (ClientException $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Cannot create table "%s" definition in Storage API: %s',
                    $destination->getTableName(),
                    json_encode((array) $e->getContextParams())
                ),
                $e->getCode(),
                $e
            );
        }
    }

    private function checkDevBucketMetadata(MappingDestination $destination): void
    {
        if (!$this->clientWrapper->hasBranch()) {
            return;
        }
        $bucketId = $destination->getBucketId();
        $metadata = new Metadata($this->clientWrapper->getBasicClient());
        try {
            foreach ($metadata->listBucketMetadata($bucketId) as $metadatum) {
                if (($metadatum['key'] === TableWriter::KBC_LAST_UPDATED_BY_BRANCH_ID) ||
                    ($metadatum['key'] === TableWriter::KBC_CREATED_BY_BRANCH_ID)) {
                    if ((string) $metadatum['value'] === (string) $this->clientWrapper->getBranchId()) {
                        return;
                    }

                    throw new InvalidOutputException(sprintf(
                        'Trying to create a table in the development bucket "%s" on branch ' .
                        '"%s" (ID "%s"). The bucket metadata marks it as assigned to branch with ID "%s".',
                        $bucketId,
                        $this->clientWrapper->getBranchName(),
                        $this->clientWrapper->getBranchId(),
                        $metadatum['value']
                    ));
                }
            }
        } catch (ClientException $e) {
            // this is Ok, if the bucket it does not exists, it can't have wrong metadata
            if ($e->getCode() === 404) {
                return;
            }

            throw $e;
        }
        throw new InvalidOutputException(sprintf(
            'Trying to create a table in the development ' .
            'bucket "%s" on branch "%s" (ID "%s"), but the bucket is not assigned to any development branch.',
            $bucketId,
            $this->clientWrapper->getBranchName(),
            $this->clientWrapper->getBranchId()
        ));
    }

    private function getCreatedMetadata(array $systemMetadata): array
    {
        $metadata[] = [
            'key' => TableWriter::KBC_CREATED_BY_COMPONENT_ID,
            'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_CONFIGURATION_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_CONFIGURATION_ROW_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_CREATED_BY_BRANCH_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }

    private function getUpdatedMetadata(array $systemMetadata): array
    {
        $metadata[] = [
            'key' => TableWriter::KBC_LAST_UPDATED_BY_COMPONENT_ID,
            'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_COMPONENT_ID],
        ];
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_CONFIGURATION_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ID],
            ];
        }
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_CONFIGURATION_ROW_ID],
            ];
        }
        if (!empty($systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID])) {
            $metadata[] = [
                'key' => TableWriter::KBC_LAST_UPDATED_BY_BRANCH_ID,
                'value' => $systemMetadata[AbstractWriter::SYSTEM_KEY_BRANCH_ID],
            ];
        }
        return $metadata;
    }
}
