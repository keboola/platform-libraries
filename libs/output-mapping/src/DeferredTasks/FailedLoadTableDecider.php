<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\DeferredTasks;

use Keboola\OutputMapping\Writer\Helper\DescriptionHelper;
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
        $metadata = $tableInfo['metadata'];
        if ($tableInfo['isTyped']) {
            $metadata = array_filter(
                $metadata,
                fn($m) => $m['key'] !== 'KBC.dataTypesEnabled' || $m['provider'] !== 'storage',
            );
        }

        // A description passed in the create-table-definition payload makes Storage write a KBC.description
        // row under the "storage" provider at create time, i.e. before anything is loaded. It therefore says
        // nothing about the table having been used and must not stop the drop of a failed empty table. This
        // is intentionally outside the isTyped branch above - the non-typed create-table-definition payload
        // (TableDefinitionFromColumns) can carry a description too.
        $metadata = array_filter(
            $metadata,
            fn($m) => $m['key'] !== DescriptionHelper::DESCRIPTION_METADATA_KEY || $m['provider'] !== 'storage',
        );

        if ($task->isUsingFreshlyCreatedTable() && // most important
            ($tableInfo['rowsCount'] === 0 || $tableInfo['rowsCount'] === null) && // seems both are possible 🙄
            (count($metadata) === 0) // at this point there should be no metadata, they're set after load
        ) {
            $logger->warning(sprintf('Failed to load table "%s". Dropping table.', $task->getDestinationTableName()));
            return true;
        }
        return false;
    }
}
