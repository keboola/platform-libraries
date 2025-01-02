<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\TableNotFoundException;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class TableStructureValidatorFactory
{
    public function __construct(readonly LoggerInterface $logger, readonly ClientWrapper $client)
    {
    }

    public function ensureStructureValidator(string $tableId): AbstractTableStructureValidator
    {
        try {
            $table = $this
                ->client
                ->getTableAndFileStorageClient()
                ->getTable($tableId);
        } catch (ClientException $e) {
            if ($e->getCode() === 404) {
                throw new TableNotFoundException($e->getMessage(), $e->getCode(), $e);
            }
            throw new InvalidOutputException($e->getMessage(), $e->getCode(), $e);
        }

        if ($table['isTyped']) {
            return new TypedTableStructureValidator($this->logger, $table);
        } else {
            return new TableStructureValidator($this->logger, $table);
        }
    }
}
