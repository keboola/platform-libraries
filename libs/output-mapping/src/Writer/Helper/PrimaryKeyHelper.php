<?php

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;

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
                })
            )
        );
    }

    /**
     * @param array $tableInfo
     * @param array $config
     * @param LoggerInterface $logger
     * @return bool
     */
    public static function modifyPrimaryKeyDecider(LoggerInterface $logger, array $tableInfo, array $config)
    {
        $configPK = self::normalizeKeyArray($logger, $config['primary_key']);
        if (count($tableInfo['primaryKey']) !== count($configPK)) {
            return true;
        }
        if (count(array_intersect($tableInfo['primaryKey'], $configPK)) !== count($tableInfo['primaryKey'])) {
            return true;
        }
        return false;
    }

    /**
     * @param string $tableId
     * @param array $tablePrimaryKey
     * @param array $configPrimaryKey
     */
    public static function modifyPrimaryKey(
        LoggerInterface $logger,
        Client $client,
        $tableId,
        array $tablePrimaryKey,
        array $configPrimaryKey
    ) {
        $logger->warning(sprintf(
            'Modifying primary key of table "%s" from "%s" to "%s".',
            $tableId,
            join(', ', $tablePrimaryKey),
            join(', ', $configPrimaryKey)
        ));
        if (self::removePrimaryKey($logger, $client, $tableId, $tablePrimaryKey)) {
            // modify primary key
            try {
                if (count($configPrimaryKey)) {
                    $client->createTablePrimaryKey($tableId, $configPrimaryKey);
                }
            } catch (\Exception $e) {
                // warn and try to rollback to original state
                $logger->warning(
                    "Error changing primary key of table {$tableId}: " . $e->getMessage()
                );
                if (count($tablePrimaryKey) > 0) {
                    $client->createTablePrimaryKey($tableId, $tablePrimaryKey);
                }
            }
        }
    }

    /**
     * @param LoggerInterface $logger
     * @param Client $client
     * @param string $tableId
     * @param array $tablePrimaryKey
     * @return bool
     */
    private static function removePrimaryKey(LoggerInterface $logger, Client $client, $tableId, array $tablePrimaryKey)
    {
        if (count($tablePrimaryKey) > 0) {
            try {
                $client->removeTablePrimaryKey($tableId);
            } catch (\Exception $e) {
                // warn and go on
                $logger->warning(
                    "Error deleting primary key of table {$tableId}: " . $e->getMessage()
                );
                return false;
            }
        }
        return true;
    }
}
