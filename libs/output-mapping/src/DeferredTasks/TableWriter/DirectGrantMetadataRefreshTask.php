<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks\TableWriter;

use Keboola\OutputMapping\DeferredTasks\DeferredTaskInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use LogicException;

/**
 * Tables written through direct grants are already in their Storage bucket when output mapping runs, so there is
 * no table load job for them. This task enqueues the Storage job which refreshes their metadata and dispatches the
 * table import events downstream triggers listen to.
 */
class DirectGrantMetadataRefreshTask implements DeferredTaskInterface
{
    /** @var string[] */
    private array $storageJobIds = [];

    private bool $started = false;

    public function __construct(private readonly int $workspaceId)
    {
    }

    /**
     * Storage enqueues a single refreshStorageBuckets job covering every direct-grant bucket of the workspace, but
     * the endpoint is typed as a list of jobs and will grow the jobs of the remaining unload strategies, so every
     * returned id has to be awaited. An empty list is a valid answer - a workspace with no direct-grant output
     * mapping in its configuration has nothing to refresh.
     */
    public function start(ClientWrapper $clientWrapper): void
    {
        // the workspace is a branch object, so the refresh has to be enqueued through the branch client
        $workspaces = new Workspaces($clientWrapper->getBranchClient());

        try {
            $jobIds = $workspaces->queueUnload($this->workspaceId, ['only-direct-grants' => true]);
        } catch (ClientException $e) {
            // only a 4xx is the user's fault; a connection failure or a client-side error carries code 0
            if ($e->getCode() >= 400 && $e->getCode() < 500) {
                throw new InvalidOutputException(
                    sprintf('Failed to refresh metadata of direct-grant tables: %s', $e->getMessage()),
                    $e->getCode(),
                    $e,
                );
            }

            throw $e;
        }

        $this->storageJobIds = array_map(strval(...), $jobIds);
        $this->started = true;
    }

    public function getStorageJobIds(): array
    {
        if (!$this->started) {
            throw new LogicException('Metadata refresh of direct-grant tables has not been started yet.');
        }

        return $this->storageJobIds;
    }

    public function getFailedJobError(string $storageJobId, array $jobResult): string
    {
        return sprintf(
            'Failed to refresh metadata of direct-grant tables (Storage job "%s"): %s',
            $storageJobId,
            $jobResult['error']['message'],
        );
    }
}
