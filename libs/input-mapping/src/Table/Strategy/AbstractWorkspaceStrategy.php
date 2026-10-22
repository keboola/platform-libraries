<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\Exception\InputOperationException;
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
     * Plan and submit the workspace table loads as a single async job, returning a queue handle for
     * later completion with waitForTableLoadCompletion().
     *
     * The load type per item (CLONE / VIEW / COPY) is resolved server-side by the workspace
     * input-mapping-load endpoint, which accepts the keboola/input-mapping Configuration\Table payload
     * (snake_case) unchanged. Each table's parsed configuration is passed through as-is (including any
     * explicit load_type), so no client-side load-type decision is made here.
     *
     * @param RewrittenInputTableOptions[] $tables
     */
    public function prepareAndExecuteTableLoads(array $tables, bool $preserve): WorkspaceLoadQueue
    {
        $inputs = [];
        foreach ($tables as $table) {
            $inputs[] = $table->getStorageApiWorkspaceLoadConfiguration($this->tablesState);
        }

        // Nothing to load and the workspace is kept as-is: no job needed.
        if ($inputs === [] && $preserve) {
            return new WorkspaceLoadQueue([], static::class, $this->destination);
        }

        if (!$preserve) {
            $this->logger->info('Cleaning workspace and loading tables.');
        }
        $this->logger->info(sprintf('Loading %s tables to workspace.', count($inputs)));

        // preserve: 0 = clean + load, 1 = load only
        $jobId = $this->createWorkspaces()->queueInputMappingLoad(
            (int) $this->dataStorage->getWorkspaceId(),
            [
                'input' => $inputs,
                'preserve' => $preserve ? 1 : 0,
            ],
        );

        return new WorkspaceLoadQueue([
            new WorkspaceLoadJob((string) $jobId, $tables),
        ], static::class, $this->destination);
    }

    protected function createWorkspaces(): Workspaces
    {
        return new Workspaces($this->clientWrapper->getBranchClient());
    }
}
