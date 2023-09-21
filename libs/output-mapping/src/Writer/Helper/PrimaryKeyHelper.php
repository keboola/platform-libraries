<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;
use Throwable;

class PrimaryKeyHelper
{
    /**
     * @param array $keys
     * @param LoggerInterface $logger
     * @return array
     */
    public static function normalizeKeyArray(LoggerInterface $logger, array $keys)
    {
        return array_map(
            function ($key) {
                return trim($key);
            },
            array_unique(
                array_filter($keys, function ($col) use ($logger) {
                    if ($col !== '') {
                        return true;
                    }
                    $logger->warning('Found empty column name in key array.');
                    return false;
                }),
            ),
        );
    }

    public static function modifyPrimaryKeyDecider(
        LoggerInterface $logger,
        array $currentTableInfo,
        array $newTableConfiguration,
    ): bool {
        $configPK = self::normalizeKeyArray($logger, $newTableConfiguration['primary_key']);
        if (count($currentTableInfo['primaryKey']) !== count($configPK)) {
            return true;
        }
        $currentTablePkColumnsCount = count($currentTableInfo['primaryKey']);
        if (count(array_intersect($currentTableInfo['primaryKey'], $configPK)) !== $currentTablePkColumnsCount) {
            return true;
        }
        return false;
    }

    public static function modifyPrimaryKey(
        LoggerInterface $logger,
        Client $client,
        string $tableId,
        array $tablePrimaryKey,
        array $configPrimaryKey,
    ): void {
        $logger->warning(sprintf(
            'Modifying primary key of table "%s" from "%s" to "%s".',
            $tableId,
            join(', ', $tablePrimaryKey),
            join(', ', $configPrimaryKey),
        ));
        if (self::removePrimaryKey($logger, $client, $tableId, $tablePrimaryKey)) {
            // modify primary key
            try {
                if (count($configPrimaryKey)) {
                    $client->createTablePrimaryKey($tableId, $configPrimaryKey);
                }
            } catch (Throwable $e) {
                // warn and try to rollback to original state
                $logger->warning(
                    "Error changing primary key of table {$tableId}: " . $e->getMessage(),
                );
                if (count($tablePrimaryKey) > 0) {
                    $client->createTablePrimaryKey($tableId, $tablePrimaryKey);
                }
            }
        }
    }

    private static function removePrimaryKey(
        LoggerInterface $logger,
        Client $client,
        string $tableId,
        array $tablePrimaryKey,
    ): bool {
        if (count($tablePrimaryKey) > 0) {
            try {
                $client->removeTablePrimaryKey($tableId);
            } catch (Throwable $e) {
                // warn and go on
                $logger->warning(
                    "Error deleting primary key of table {$tableId}: " . $e->getMessage(),
                );
                return false;
            }
        }
        return true;
    }
}
