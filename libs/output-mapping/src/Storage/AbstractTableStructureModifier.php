<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractTableStructureModifier
{
    protected readonly Client|BranchAwareClient $client;

    public function __construct(
        ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
    ) {
        $this->client = $clientWrapper->getTableAndFileStorageClient();
    }

    /**
     * @param array $keys
     * @return array
     */
    protected function normalizeKeyArray(array $keys)
    {
        $logger = $this->logger;
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

    protected function modifyPrimaryKeyDecider(
        array $currentTablePrimaryKey,
        array $newTableConfigurationPrimaryKey,
    ): bool {
        $configPK = $this->normalizeKeyArray($newTableConfigurationPrimaryKey);
        if (count($currentTablePrimaryKey) !== count($configPK)) {
            return true;
        }
        $currentTablePkColumnsCount = count($currentTablePrimaryKey);
        if (count(array_intersect($currentTablePrimaryKey, $configPK)) !== $currentTablePkColumnsCount) {
            return true;
        }
        return false;
    }

    protected function modifyPrimaryKey(
        string $tableId,
        array $tablePrimaryKey,
        array $configPrimaryKey,
    ): void {
        $this->logger->warning(sprintf(
            'Modifying primary key of table "%s" from "%s" to "%s".',
            $tableId,
            join(', ', $tablePrimaryKey),
            join(', ', $configPrimaryKey),
        ));
        if ($this->removePrimaryKey($tableId, $tablePrimaryKey)) {
            // modify primary key
            try {
                if (count($configPrimaryKey)) {
                    $this->client->createTablePrimaryKey($tableId, $configPrimaryKey);
                }
            } catch (ClientException $e) {
                // warn and try to rollback to original state
                $this->logger->warning(
                    "Error changing primary key of table {$tableId}: " . $e->getMessage(),
                );
                if (count($tablePrimaryKey) > 0) {
                    $this->client->createTablePrimaryKey($tableId, $tablePrimaryKey);
                }
            }
        }
    }

    protected function removePrimaryKey(
        string $tableId,
        array $tablePrimaryKey,
    ): bool {
        if (count($tablePrimaryKey) > 0) {
            try {
                $this->client->removeTablePrimaryKey($tableId);
            } catch (ClientException $e) {
                // warn and go on
                $this->logger->warning(
                    "Error deleting primary key of table {$tableId}: " . $e->getMessage(),
                );
                return false;
            }
        }
        return true;
    }
}
