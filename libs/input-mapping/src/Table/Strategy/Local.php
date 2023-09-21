<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\StorageApi\TableExporter;

class Local extends AbstractStrategy
{
    public const DEFAULT_MAX_EXPORT_SIZE_BYTES = 100000000000;
    public const EXPORT_SIZE_LIMIT_NAME = 'components.max_export_size_bytes';

    public function downloadTable(RewrittenInputTableOptions $table): array
    {
        $tokenInfo = $this->clientWrapper->getBranchClient()->verifyToken();
        $exportLimit = self::DEFAULT_MAX_EXPORT_SIZE_BYTES;
        if (!empty($tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME])) {
            $exportLimit = $tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME]['value'];
        }

        $file = $this->ensurePathDelimiter($this->dataStorage->getPath()) .
            $this->getDestinationFilePath($this->destination, $table);
        $tableInfo = $table->getTableInfo();
        if ($tableInfo['dataSizeBytes'] > $exportLimit) {
            throw new InvalidInputException(sprintf(
                'Table "%s" with size %s bytes exceeds the input mapping limit of %s bytes. ' .
                'Please contact support to raise this limit',
                $table->getSource(),
                $tableInfo['dataSizeBytes'],
                $exportLimit,
            ));
        }

        $this->manifestCreator->writeTableManifest(
            $tableInfo,
            $this->ensurePathDelimiter($this->metadataStorage->getPath()) .
                $this->getDestinationFilePath($this->destination, $table) . '.manifest',
            $table->getColumnNamesFromTypes(),
            $this->format,
        );
        return [
            'tableId' => $table->getSource(),
            'destination' => $file,
            'exportOptions' => $table->getStorageApiExportOptions($this->tablesState),
        ];
    }

    public function handleExports(array $exports, bool $preserve): array
    {
        $tableExporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $this->logger->info('Processing ' . count($exports) . ' local table exports.');
        return $tableExporter->exportTables($exports);
    }
}
