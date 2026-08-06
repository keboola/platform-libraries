<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\StorageApiBranch\ClientWrapper;

/**
 * A Storage job enqueued by output mapping and awaited by LoadTableQueue.
 */
interface DeferredTaskInterface
{
    /**
     * Enqueues the Storage job(s) of this task.
     *
     * A Storage failure which is the user's fault must be converted to an InvalidOutputException here.
     */
    public function start(ClientWrapper $clientWrapper): void;

    /**
     * @return string[] ids of the Storage jobs enqueued by start()
     */
    public function getStorageJobIds(): array;

    /**
     * @param array $jobResult job detail of a job which ended with an error
     * @return string message to report to the user
     */
    public function getFailedJobError(string $storageJobId, array $jobResult): string;
}
