<?php

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;

class PrimaryKeyHelper
{
    /**
     * @param LoggerInterface $logger
     * @param array $tableInfo
     * @param array $config
     */
    public static function validatePrimaryKeyAgainstTable(LoggerInterface $logger, $tableInfo = [], $config = [])
    {
        // primary key
        $configPK = self::normalizePrimaryKey($logger, $config['primary_key']);
        if (count($configPK) > 0 || count($tableInfo['primaryKey']) > 0) {
            if (count(array_diff($tableInfo['primaryKey'], $configPK)) > 0 ||
                count(array_diff($configPK, $tableInfo['primaryKey'])) > 0
            ) {
                throw new InvalidOutputException(sprintf(
                    'Output mapping does not match destination table: primary key "%s" does not match "%s" in "%s".',
                    join(', ', $configPK),
                    join(', ', $tableInfo['primaryKey']),
                    $config['destination']
                ));
            }
        }
    }

    /**
     * @param array $primaryKey
     * @param LoggerInterface $logger
     * @return array
     */
    public static function normalizePrimaryKey(LoggerInterface $logger, array $primaryKey)
    {
        return array_map(
            function ($primaryKey) {
                return trim($primaryKey);
            },
            array_unique(
                array_filter($primaryKey, function ($col) use ($logger) {
                    if ($col !== '') {
                        return true;
                    }
                    $logger->warning('Empty primary key found.');
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
        $configPK = self::normalizePrimaryKey($logger, $config['primary_key']);
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
