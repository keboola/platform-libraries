<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class FailedLoadTableDecider
{
    public static function decideTableDelete(
        LoggerInterface $logger,
        ClientWrapper $clientWrapper,
        LoadTableTaskInterface $task,
    ): bool {
        try {
            $tableInfo = $clientWrapper->getTableAndFileStorageClient()->getTable($task->getDestinationTableName());
        } catch (ClientException $e) {
            // likely the table doesn't exist, but any other error really prevents us from positive decision
            return false;
        }
        if ($task->isUsingFreshlyCreatedTable() && // most important
            ($tableInfo['rowsCount'] === 0 || $tableInfo['rowsCount'] === null) && // seems both are possible 🙄
            (count($tableInfo['metadata']) === 0) // at this point there should be no metadata, they're set after load
        ) {
            $logger->warning(sprintf('Failed to load table "%s". Dropping table.', $task->getDestinationTableName()));
            return true;
        }
        return false;
    }
}
