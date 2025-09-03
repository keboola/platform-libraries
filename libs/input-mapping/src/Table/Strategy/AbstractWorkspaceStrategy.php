<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\Helper\LoadTypeDecider;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractWorkspaceStrategy extends AbstractStrategy
{
    protected readonly WorkspaceStagingInterface $dataStorage;
    protected readonly ManifestCreator $manifestCreator;

    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        StagingInterface $dataStorage,
        protected readonly FileStagingInterface $metadataStorage,
        protected readonly InputTableStateList $tablesState,
        protected readonly string $destination,
        protected readonly FileFormat $format,
    ) {
        if (!$dataStorage instanceof WorkspaceStagingInterface) {
            throw new InvalidArgumentException('Data storage must be instance of WorkspaceStagingInterface');
        }

        $this->dataStorage = $dataStorage;
        $this->manifestCreator = new ManifestCreator();
    }

    abstract public function getWorkspaceType(): string;

    protected function getMetadataStorage(): FileStagingInterface
    {
        return $this->metadataStorage;
    }

    protected function getDestination(): string
    {
        return $this->destination;
    }

    public function downloadTable(RewrittenInputTableOptions $table): array
    {
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        LoadTypeDecider::checkViableLoadMethod(
            $table->getTableInfo(),
            $this->getWorkspaceType(),
            $loadOptions,
            $this->clientWrapper->getToken()->getProjectId(),
        );

        if (LoadTypeDecider::canClone($table->getTableInfo(), $this->getWorkspaceType(), $loadOptions)) {
            $this->logger->info(sprintf('Table "%s" will be cloned.', $table->getSource()));
            return [
                'table' => $table,
                'type' => WorkspaceLoadType::CLONE->value,
            ];
        }
        if (LoadTypeDecider::canUseView(
            $table->getTableInfo(),
            $this->getWorkspaceType(),
        )) {
            $this->logger->info(sprintf('Table "%s" will be created as view.', $table->getSource()));
            return [
                'table' => [$table, $loadOptions],
                'type' => WorkspaceLoadType::VIEW->value,
            ];
        }
        $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
        return [
            'table' => [$table, $loadOptions],
            'type' => WorkspaceLoadType::COPY->value,
        ];
    }

    public function handleExports(array $exports, bool $preserve): array
    {
        $cloneInputs = [];
        $copyInputs = [];
        $workspaceTables = [];

        foreach ($exports as $export) {
            if ($export['type'] === WorkspaceLoadType::CLONE->value) {
                /** @var RewrittenInputTableOptions $table */
                $table = $export['table'];
                $cloneInput = [
                    'source' => $table->getSource(),
                    'destination' => $table->getDestination(),
                    'sourceBranchId' => $table->getSourceBranchId(),
                    'overwrite' => $table->getOverwrite(),
                    'dropTimestampColumn' => !$table->keepInternalTimestampColumn(),
                ];
                if ($table->getSourceBranchId() !== null) {
                    // practically, sourceBranchId should never be null, but i'm not able to make that statically safe
                    // and passing null causes application error in connection, so here is a useless condition.
                    $cloneInput['sourceBranchId'] = $table->getSourceBranchId();
                }
                $cloneInputs[] = $cloneInput;
                $workspaceTables[] = $table;
            }
            if (in_array($export['type'], [WorkspaceLoadType::COPY->value, WorkspaceLoadType::VIEW->value], true)) {
                [$table, $exportOptions] = $export['table'];
                if ($table->getSourceBranchId() !== null) {
                    // practically, sourceBranchId should never be null, but i'm not able to make that statically safe
                    // and passing null causes application error in connection, so here is a useless condition.
                    $exportOptions['sourceBranchId'] = $table->getSourceBranchId();
                }
                $copyInput = array_merge(
                    [
                        'source' => $table->getSource(),
                        'destination' => $table->getDestination(),
                    ],
                    $exportOptions,
                );

                if ($table->isUseView() || $export['type'] === WorkspaceLoadType::VIEW->value) {
                    $copyInput['useView'] = true;
                }

                $workspaceTables[] = $table;
                $copyInputs[] = $copyInput;
            }
        }

        $cloneJobResult = [];
        $copyJobResult = [];
        $hasBeenCleaned = false;

        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());

        if ($cloneInputs) {
            $this->logger->info(
                sprintf('Cloning %s tables to workspace.', count($cloneInputs)),
            );
            // here we are waiting for the jobs to finish. handleAsyncTask = true
            // We need to process clone and copy jobs separately because there is no lock on the table and there
            // is a race between the clone and copy jobs which can end in an error that the table already exists.
            // Full description of the issue here: https://keboola.atlassian.net/wiki/spaces/KB/pages/2383511594/Input+mapping+to+workspace+Consolidation#Context
            $jobId = $workspaces->queueWorkspaceCloneInto(
                (int) $this->dataStorage->getWorkspaceId(),
                [
                    'input' => $cloneInputs,
                    'preserve' => $preserve ? 1 : 0,
                ],
            );
            $cloneJobResult = $this->clientWrapper->getBranchClient()->handleAsyncTasks([$jobId]);
            if (!$preserve) {
                $hasBeenCleaned = true;
            }
        }

        if ($copyInputs) {
            $this->logger->info(
                sprintf('Copying %s tables to workspace.', count($copyInputs)),
            );
            $jobId = $workspaces->queueWorkspaceLoadData(
                (int) $this->dataStorage->getWorkspaceId(),
                [
                    'input' => $copyInputs,
                    'preserve' => !$hasBeenCleaned && !$preserve ? 0 : 1,
                ],
            );
            $copyJobResult = $this->clientWrapper->getBranchClient()->handleAsyncTasks([$jobId]);
        }
        $jobResults = array_merge($cloneJobResult, $copyJobResult);
        $this->logger->info('Processed ' . count($jobResults) . ' workspace exports.');

        foreach ($workspaceTables as $table) {
            $manifestPath = $this->getManifestPath($table);
            $this->manifestCreator->writeTableManifest(
                $table->getTableInfo(),
                $manifestPath,
                $table->getColumnNamesFromTypes(),
                $this->format,
            );
        }
        return $jobResults;
    }

    /**
     * Phase 1: Prepare - Analyze tables and create workspace load instructions
     * Determines how each table from Table Storage should be loaded into Workspace
     *
     * @param RewrittenInputTableOptions[] $tables
     * @return WorkspaceTableLoadInstruction[]
     */
    public function prepareTableLoadsToWorkspace(array $tables): array
    {
        $instructions = [];

        foreach ($tables as $table) {
            $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);

            // Validate that table can be loaded to this workspace type
            LoadTypeDecider::checkViableLoadMethod(
                $table->getTableInfo(),
                $this->getWorkspaceType(),
                $loadOptions,
                $this->clientWrapper->getToken()->getProjectId(),
            );

            // Determine optimal load method from Table Storage to Workspace
            if (LoadTypeDecider::canClone($table->getTableInfo(), $this->getWorkspaceType(), $loadOptions)) {
                $this->logger->info(sprintf('Table "%s" will be cloned.', $table->getSource()));
                $instructions[] = new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::CLONE,
                    $table,
                    null,
                );
            } elseif (LoadTypeDecider::canUseView($table->getTableInfo(), $this->getWorkspaceType())) {
                $this->logger->info(sprintf('Table "%s" will be created as view.', $table->getSource()));
                $instructions[] = new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::VIEW,
                    $table,
                    $loadOptions,
                );
            } else {
                $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
                $instructions[] = new WorkspaceTableLoadInstruction(
                    WorkspaceLoadType::COPY,
                    $table,
                    $loadOptions,
                );
            }
        }

        return $instructions;
    }
}
