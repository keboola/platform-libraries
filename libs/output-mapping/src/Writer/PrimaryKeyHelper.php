<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;

class PrimaryKeyHelper
{
    /**
     * @var Client
     */
    private $client;

    /**
     * @var LoggerInterface
     */
    private $logger;

    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->client = $client;
        $this->logger = $logger;
    }

    /**
     * @param array $tableInfo
     * @param array $config
     */
    public function validatePrimaryKeyAgainstTable($tableInfo = [], $config = [])
    {
        // primary key
        $configPK = self::normalizePrimaryKey($config['primary_key'], $this->logger);
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
     * @param array $tableInfo
     * @param array $config
     * @param LoggerInterface $logger
     * @return bool
     */
    public static function modifyPrimaryKeyDecider(array $tableInfo, array $config, LoggerInterface $logger)
    {
        $configPK = self::normalizePrimaryKey($config['primary_key'], $logger);
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
    public function modifyPrimaryKey($tableId, array $tablePrimaryKey, array $configPrimaryKey)
    {
        $this->logger->warning(sprintf(
            'Modifying primary key of table "%s" from "%s" to "%s".',
            $tableId,
            join(', ', $tablePrimaryKey),
            join(', ', $configPrimaryKey)
        ));
        if ($this->removePrimaryKey($tableId, $tablePrimaryKey)) {
            // modify primary key
            try {
                if (count($configPrimaryKey)) {
                    $this->client->createTablePrimaryKey($tableId, $configPrimaryKey);
                }
            } catch (\Exception $e) {
                // warn and try to rollback to original state
                $this->logger->warning(
                    "Error changing primary key of table {$tableId}: " . $e->getMessage()
                );
                if (count($tablePrimaryKey) > 0) {
                    $this->client->createTablePrimaryKey($tableId, $tablePrimaryKey);
                }
            }
        }
    }

    /**
     * @param string $tableId
     * @param array $tablePrimaryKey
     * @return bool
     */
    private function removePrimaryKey($tableId, array $tablePrimaryKey)
    {
        if (count($tablePrimaryKey) > 0) {
            try {
                $this->client->removeTablePrimaryKey($tableId);
            } catch (\Exception $e) {
                // warn and go on
                $this->logger->warning(
                    "Error deleting primary key of table {$tableId}: " . $e->getMessage()
                );
                return false;
            }
        }
        return true;
    }

    /**
     * @param array $pKey
     * @param LoggerInterface $logger
     * @return array
     */
    public static function normalizePrimaryKey(array $pKey, $logger)
    {
        return array_map(
            function ($pKey) {
                return trim($pKey);
            },
            array_unique(
                array_filter($pKey, function ($col) use ($logger) {
                    if ($col !== '') {
                        return true;
                    }
                    $logger->warning('Empty primary key found');
                    return false;
                })
            )
        );
    }
}
