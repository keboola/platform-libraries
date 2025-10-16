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
        $allInputs = [];
        $workspaceTables = [];

        foreach ($exports as $export) {
            if ($export['type'] === WorkspaceLoadType::CLONE->value) {
                /** @var RewrittenInputTableOptions $table */
                $table = $export['table'];
                $allInputs[] = $this->buildCloneInput($table);
                $workspaceTables[] = $table;
            }
            if (in_array($export['type'], [WorkspaceLoadType::COPY->value, WorkspaceLoadType::VIEW->value], true)) {
                [$table, $exportOptions] = $export['table'];
                $loadType = WorkspaceLoadType::from($export['type']);
                $allInputs[] = $this->buildCopyInput($table, $exportOptions, $loadType);
                $workspaceTables[] = $table;
            }
        }

        if (empty($allInputs)) {
            return [];
        }

        $workspaces = $this->createWorkspaces();

        $this->logger->info(sprintf(
            'Loading %d tables to workspace%s.',
            count($allInputs),
            !$preserve ? ' with clean' : '',
        ));

        $jobId = $workspaces->queueWorkspaceLoadData(
            (int) $this->dataStorage->getWorkspaceId(),
            [
                'input' => $allInputs,
                'preserve' => $preserve ? 1 : 0,
            ],
        );

        $jobResults = $this->clientWrapper->getBranchClient()->handleAsyncTasks([$jobId]);

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
     */
    public function executeTableLoadsToWorkspace(WorkspaceLoadPlan $plan): WorkspaceLoadQueue
    {
        $workspaces = $this->createWorkspaces();
        $workspaceId = (int) $this->dataStorage->getWorkspaceId();

        $allInputs = [];
        $allTables = [];

        foreach ($plan->getAllInstructions() as $instruction) {
            if ($instruction->loadType === WorkspaceLoadType::CLONE) {
                $allInputs[] = $this->buildCloneInput($instruction->table);
            } else {
                $allInputs[] = $this->buildCopyInput(
                    $instruction->table,
                    $instruction->loadOptions ?? [],
                    $instruction->loadType,
                );
            }
            $allTables[] = $instruction->table;
        }

        if (empty($allInputs)) {
            return new WorkspaceLoadQueue([]);
        }

        $stats = $plan->getStatistics();
        $this->logger->info(sprintf(
            'Loading %d tables to workspace (%d clone, %d copy, %d view)%s',
            $stats['total'],
            $stats['clone'],
            $stats['copy'],
            $stats['view'],
            !$plan->preserve ? ' with clean' : '',
        ));

        $jobId = $workspaces->queueWorkspaceLoadData($workspaceId, [
            'input' => $allInputs,
            'preserve' => $plan->preserve ? 1 : 0,
        ]);

        return new WorkspaceLoadQueue([
            new WorkspaceLoadJob((string) $jobId, null, $allTables),
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
        if ($table->getSourceBranchId() !== null) {
            $loadOptions['sourceBranchId'] = $table->getSourceBranchId();
        }

        $copyInput = array_merge([
            'source' => $table->getSource(),
            'destination' => $table->getDestination(),
            'loadType' => $loadType->value,
        ], $loadOptions);

        if ($loadType === WorkspaceLoadType::VIEW || $table->isUseView()) {
            $copyInput['useView'] = true;
        }

        return $copyInput;
    }

    private function buildCloneInput(RewrittenInputTableOptions $table): array
    {
        return [
            'source' => $table->getSource(),
            'destination' => $table->getDestination(),
            'loadType' => WorkspaceLoadType::CLONE->value,
            'sourceBranchId' => $table->getSourceBranchId(),
            'overwrite' => $table->getOverwrite(),
            'dropTimestampColumn' => !$table->keepInternalTimestampColumn(),
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
