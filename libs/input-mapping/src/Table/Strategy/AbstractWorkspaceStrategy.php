<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Helper\LoadTypeDecider;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\InputMapping\Helper\PathHelper;
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

    protected function materializeTableLoads(TableLoadQueueInterface $queue, array $jobResults): void
    {
        if (!$queue instanceof WorkspaceLoadQueue) {
            throw new InputOperationException('Workspace strategy requires WorkspaceLoadQueue.');
        }

        $this->logger->info('Processed ' . count($jobResults) . ' workspace exports.');

        foreach ($queue->getAllTables() as $table) {
            $manifestPath = PathHelper::getManifestPath(
                $this->metadataStorage,
                $this->destination,
                $table,
            );
            $this->manifestCreator->writeTableManifest(
                $table->getTableInfo(),
                $manifestPath,
                $table->getColumnNamesFromTypes(),
                $this->format,
            );
        }
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
            $loadType = $this->decideTableLoadMethod($table, $loadOptions);

            $instructionLoadOptions = $loadType === WorkspaceLoadType::CLONE ? null : $loadOptions;
            $instructions[] = new WorkspaceTableLoadInstruction(
                $loadType,
                $table,
                $instructionLoadOptions,
            );
        }

        return $instructions;
    }

    /**
     * Phase 2: Execute - Submit single async job to load all tables from Table Storage into Workspace
     * All operations (CLEAN, CLONE, COPY, VIEW) are batched into a single API call
     *
     * This method is used by SQL editor API that enqueues workspace loads asynchronously.
     * Caller does not wait for load completion and uses the new unified API format with loadType parameter.
     */
    public function executeTableLoadsToWorkspace(WorkspaceLoadPlan $plan): WorkspaceLoadQueue
    {
        $workspaces = $this->createWorkspaces();
        $workspaceId = (int) $this->dataStorage->getWorkspaceId();

        // Collect all inputs in order: CLONE operations first, then COPY/VIEW operations
        $allInputs = [];
        $allTables = [];
        $cloneCount = 0;
        $copyCount = 0;

        // Step 1: Add CLONE operations (must be first to maintain proper ordering)
        $cloneInstructions = $plan->getCloneInstructions();
        foreach ($cloneInstructions as $instruction) {
            $allInputs[] = $this->buildCloneInputWithLoadType($instruction->table);
            $allTables[] = $instruction->table;
            $cloneCount++;
        }

        // Step 2: Add COPY/VIEW operations
        $copyInstructions = $plan->getCopyInstructions();
        foreach ($copyInstructions as $instruction) {
            $allInputs[] = $this->buildCopyInputWithLoadType(
                $instruction->table,
                $instruction->loadOptions ?? [],
                $instruction->loadType,
            );
            $allTables[] = $instruction->table;
            $copyCount++;
        }

        // If no tables to load and preserve mode, return empty queue
        if (empty($allInputs) && $plan->preserve) {
            return new WorkspaceLoadQueue([]);
        }

        // Log operation summary
        if (!$plan->preserve) {
            $this->logger->info('Cleaning workspace and loading tables.');
        }
        if ($cloneCount > 0) {
            $this->logger->info(sprintf('Cloning %s tables to workspace.', $cloneCount));
        }
        if ($copyCount > 0) {
            $this->logger->info(sprintf('Copying %s tables to workspace.', $copyCount));
        }

        // Single API call with all operations (preserve: 0 = clean + load, preserve: 1 = load only)
        // Uses queueWorkspaceLoadData with new unified format
        $jobId = $workspaces->queueWorkspaceLoadData($workspaceId, [
            'input' => $allInputs,
            'preserve' => $plan->preserve ? 1 : 0,
        ]);

        // Return single job with all tables
        return new WorkspaceLoadQueue([
            new WorkspaceLoadJob((string) $jobId, $allTables),
        ]);
    }

    /**
     * Execute only Phase 1 & 2: Prepare and Execute workspace table loading
     * Returns WorkspaceLoadQueue for later completion with waitForTableLoadCompletion()
     *
     * @param RewrittenInputTableOptions[] $tables
     * @param bool $preserve
     * @return WorkspaceLoadQueue
     */
    public function prepareAndExecuteTableLoads(array $tables, bool $preserve): WorkspaceLoadQueue
    {
        // Phase 1: Prepare
        $instructions = $this->prepareTableLoadsToWorkspace($tables);
        $plan = new WorkspaceLoadPlan(
            $instructions,
            $preserve,
        );

        // Phase 2: Execute
        return $this->executeTableLoadsToWorkspace($plan);
    }

    /**
     * Build copy/view input for new API (executeTableLoadsToWorkspace)
     * Uses loadType parameter instead of useView boolean
     */
    private function buildCopyInputWithLoadType(
        RewrittenInputTableOptions $table,
        array $loadOptions,
        WorkspaceLoadType $loadType,
    ): array {
        // Add sourceBranchId to loadOptions
        if ($table->getSourceBranchId() !== null) {
            $loadOptions['sourceBranchId'] = $table->getSourceBranchId();
        }

        $copyInput = array_merge([
            'source' => $table->getSource(),
            'destination' => $table->getDestination(),
        ], $loadOptions);

        // Use loadType parameter (new unified API)
        $copyInput['loadType'] = $loadType->value;

        return $copyInput;
    }

    /**
     * Build clone input for new API (executeTableLoadsToWorkspace)
     * Includes loadType parameter for unified workspace loading
     */
    private function buildCloneInputWithLoadType(RewrittenInputTableOptions $table): array
    {
        return [
            'source' => $table->getSource(),
            'destination' => $table->getDestination(),
            'sourceBranchId' => $table->getSourceBranchId(),
            'overwrite' => $table->getOverwrite(),
            'dropTimestampColumn' => !$table->keepInternalTimestampColumn(),
            'loadType' => WorkspaceLoadType::CLONE->value,
        ];
    }

    private function decideTableLoadMethod(RewrittenInputTableOptions $table, array $loadOptions): WorkspaceLoadType
    {
        $explicitlyRequestsView = $table->getLoadType() === WorkspaceLoadType::VIEW->value;

        if ($this->getWorkspaceType() === 'bigquery') {
            // Validate that table can be loaded to this workspace type
            LoadTypeDecider::checkViableBigQueryLoadMethod(
                $table->getTableInfo(),
                $this->getWorkspaceType(),
            );

            // Honor an explicit load_type=VIEW request, but only after the viability check above has
            // confirmed the table can be loaded into a BigQuery workspace at all.
            if ($explicitlyRequestsView) {
                $this->logger->info(sprintf('Table "%s" will be created as view.', $table->getSource()));
                return WorkspaceLoadType::VIEW;
            }

            if (LoadTypeDecider::canUseView(
                $table->getTableInfo(),
                $this->getWorkspaceType(),
                $loadOptions,
                $this->clientWrapper->getToken()->getProjectId(),
            )) {
                $this->logger->info(sprintf('Table "%s" will be created as view.', $table->getSource()));
                return WorkspaceLoadType::VIEW;
            }
            $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
            return WorkspaceLoadType::COPY;
        }

        // Honor an explicit load_type=VIEW request before auto-deciding the load method.
        if ($explicitlyRequestsView) {
            $this->logger->info(sprintf('Table "%s" will be created as view.', $table->getSource()));
            return WorkspaceLoadType::VIEW;
        }

        if (LoadTypeDecider::canClone($table->getTableInfo(), $this->getWorkspaceType(), $loadOptions)) {
            $this->logger->info(sprintf('Table "%s" will be cloned.', $table->getSource()));
            return WorkspaceLoadType::CLONE;
        }

        if (LoadTypeDecider::canUseView(
            $table->getTableInfo(),
            $this->getWorkspaceType(),
            $loadOptions,
            $this->clientWrapper->getToken()->getProjectId(),
        )) {
            $this->logger->info(sprintf('Table "%s" will be created as view.', $table->getSource()));
            return WorkspaceLoadType::VIEW;
        }
        $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
        return WorkspaceLoadType::COPY;
    }

    protected function createWorkspaces(): Workspaces
    {
        return new Workspaces($this->clientWrapper->getBranchClient());
    }
}
