<?php

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StorageApi\TableExporter;

class Local extends AbstractStrategy
{
    const DEFAULT_MAX_EXPORT_SIZE_BYTES = 100000000000;
    const EXPORT_SIZE_LIMIT_NAME = 'components.max_export_size_bytes';

    public function downloadTable(InputTableOptions $table)
    {
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        $exportLimit = self::DEFAULT_MAX_EXPORT_SIZE_BYTES;
        if (!empty($tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME])) {
            $exportLimit = $tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME]['value'];
        }

        $file = $this->ensurePathDelimiter($this->dataStorage->getPath()) .
            $this->getDestinationFilePath($this->destination, $table);
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
        if ($tableInfo['dataSizeBytes'] > $exportLimit) {
            throw new InvalidInputException(sprintf(
                'Table "%s" with size %s bytes exceeds the input mapping limit of %s bytes. ' .
                'Please contact support to raise this limit',
                $table->getSource(),
                $tableInfo['dataSizeBytes'],
                $exportLimit
            ));
        }

        $this->manifestCreator->writeTableManifest(
            $tableInfo,
            $this->ensurePathDelimiter($this->metadataStorage->getPath()) .
                $this->getDestinationFilePath($this->destination, $table) . ".manifest",
            $table->getColumnNames(),
            $this->format
        );
        return [
            "tableId" => $table->getSource(),
            "destination" => $file,
            "exportOptions" => $table->getStorageApiExportOptions($this->tablesState),
        ];
    }

    public function handleExports($exports, $preserve)
    {
        if (!$preserve) {
            $this->logger->warning(
                'The preserve option is not supported for local staging, it is always treated as true'
            );
        }
        $tableExporter = new TableExporter($this->clientWrapper->getBasicClient());
        $this->logger->info("Processing " . count($exports) . " local table exports.");
        $tableExporter->exportTables($exports);
    }
}
