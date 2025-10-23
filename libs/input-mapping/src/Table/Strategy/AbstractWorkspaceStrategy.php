<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use InvalidArgumentException;
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

    public function downloadTable(RewrittenInputTableOptions $table): array
    {
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        $loadType = $this->decideTableLoadMethod($table, $loadOptions);

        if ($loadType === WorkspaceLoadType::CLONE) {
            return [
                'table' => $table,
                'type' => $loadType->value,
            ];
        }

        return [
            'table' => [$table, $loadOptions],
            'type' => $loadType->value,
        ];
    }

    public function handleExports(array $exports, bool $preserve): array
    {
        // Collect all inputs in order: CLONE operations first, then COPY/VIEW operations
        $allInputs = [];
        $workspaceTables = [];
        $cloneCount = 0;
        $copyCount = 0;

        // Step 1: Collect CLONE operations (must be first to maintain proper ordering)
        foreach ($exports as $export) {
            if ($export['type'] === WorkspaceLoadType::CLONE->value) {
                /** @var RewrittenInputTableOptions $table */
                $table = $export['table'];
                $allInputs[] = $this->buildCloneInput($table);
                $workspaceTables[] = $table;
                $cloneCount++;
            }
        }

        // Step 2: Collect COPY/VIEW operations
        foreach ($exports as $export) {
            if (in_array($export['type'], [WorkspaceLoadType::COPY->value, WorkspaceLoadType::VIEW->value], true)) {
                [$table, $exportOptions] = $export['table'];
                $loadType = WorkspaceLoadType::from($export['type']);
                $allInputs[] = $this->buildCopyInput($table, $exportOptions, $loadType);
                $workspaceTables[] = $table;
                $copyCount++;
            }
        }

        // If no tables to load and preserve mode, return empty results
        if (empty($allInputs) && $preserve) {
            return [];
        }

        $workspaces = $this->createWorkspaces();

        // Log operation summary
        if (!$preserve) {
            $this->logger->info('Cleaning workspace and loading tables.');
        }
        if ($cloneCount > 0) {
            $this->logger->info(sprintf('Cloning %s tables to workspace.', $cloneCount));
        }
        if ($copyCount > 0) {
            $this->logger->info(sprintf('Copying %s tables to workspace.', $copyCount));
        }

        // Single API call with all operations (preserve: 0 = clean + load, preserve: 1 = load only)
        $jobId = $workspaces->queueWorkspaceLoadData(
            (int) $this->dataStorage->getWorkspaceId(),
            [
                'input' => $allInputs,
                'preserve' => $preserve ? 1 : 0,
            ],
        );

        $jobResults = $this->clientWrapper->getBranchClient()->handleAsyncTasks([$jobId]);
        $this->logger->info('Processed ' . count($jobResults) . ' workspace exports.');

        foreach ($workspaceTables as $table) {
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
            $allInputs[] = $this->buildCloneInput($instruction->table);
            $allTables[] = $instruction->table;
            $cloneCount++;
        }

        // Step 2: Add COPY/VIEW operations
        $copyInstructions = $plan->getCopyInstructions();
        foreach ($copyInstructions as $instruction) {
            $allInputs[] = $this->buildCopyInput(
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

    private function buildCopyInput(
        RewrittenInputTableOptions $table,
        array $loadOptions,
        WorkspaceLoadType $loadType,
    ): array {
        // Add sourceBranchId to loadOptions first (preserving handleExports behavior)
        if ($table->getSourceBranchId() !== null) {
            // practically, sourceBranchId should never be null, but i'm not able to make that statically safe
            // and passing null causes application error in connection, so here is a useless condition.
            $loadOptions['sourceBranchId'] = $table->getSourceBranchId();
        }

        $copyInput = array_merge([
            'source' => $table->getSource(),
            'destination' => $table->getDestination(),
        ], $loadOptions);

        // Use loadType parameter for new unified API approach
        // If user explicitly set use_view in configuration, override loadType to VIEW
        $finalLoadType = $table->isUseView() ? WorkspaceLoadType::VIEW : $loadType;
        $copyInput['loadType'] = $finalLoadType->value;

        return $copyInput;
    }

    private function buildCloneInput(RewrittenInputTableOptions $table): array
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
        // Validate that table can be loaded to this workspace type
        LoadTypeDecider::checkViableLoadMethod(
            $table->getTableInfo(),
            $this->getWorkspaceType(),
            $loadOptions,
            $this->clientWrapper->getToken()->getProjectId(),
        );

        if (LoadTypeDecider::canClone($table->getTableInfo(), $this->getWorkspaceType(), $loadOptions)) {
            $this->logger->info(sprintf('Table "%s" will be cloned.', $table->getSource()));
            return WorkspaceLoadType::CLONE;
        }
        if (LoadTypeDecider::canUseView($table->getTableInfo(), $this->getWorkspaceType())) {
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
