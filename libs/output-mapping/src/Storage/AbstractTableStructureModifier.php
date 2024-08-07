<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\PrimaryKeyNotChangedException;
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
                $message = sprintf('Error changing primary key of table %s: %s', $tableId, $e->getMessage());
                $this->logger->warning($message);

                // rollback to original state
                if (count($tablePrimaryKey) > 0) {
                    $this->client->createTablePrimaryKey($tableId, $tablePrimaryKey);
                }

                throw new PrimaryKeyNotChangedException($message, $e->getCode(), $e);
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
