<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\StorageApi\Options\GetFileOptions;

class ABSStrategy extends AbstractStrategy
{

    public function downloadTable(InputTableOptions $table)
    {
        $exportOptions = $table->getStorageApiExportOptions($this->tablesState);
        $exportOptions['gzip'] = true;
        $jobId = $this->storageClient->queueTableExport($table->getSource(), $exportOptions);
        return [$jobId, $table];
    }

    public function handleExports($exports)
    {
        $this->logger->info("Processing " . count($exports) . " ABS table exports.");
        $jobIds = array_map(function ($export) {
            return $export[0];
        }, $exports);
        $results = $this->storageClient->handleAsyncTasks($jobIds);
        $keyedResults = [];
        foreach ($results as $result) {
            $keyedResults[$result["id"]] = $result;
        }

        /** @var InputTableOptions $table */
        foreach ($exports as $export) {
            list ($jobId, $table) = $export;
            $manifestPath = $this->getDestinationFilePath($this->destination, $table) . ".manifest";
            $tableInfo = $this->storageClient->getTable($table->getSource());
            $fileInfo = $this->storageClient->getFile(
                $keyedResults[$jobId]["results"]["file"]["id"],
                (new GetFileOptions())->setFederationToken(true)
            )
            ;
            $tableInfo["abs"] = $this->getABSInfo($fileInfo);
            $this->manifestWriter->writeTableManifest($tableInfo, $manifestPath, $table->getColumnNames());
        }
    }

    protected function getABSInfo($fileInfo)
    {
        if (empty($fileInfo['absPath'])) {
            throw new InvalidInputException('This project does not have ABS backend.');
        }
        return [
            "is_sliced" => $fileInfo["isSliced"],
            "region" => $fileInfo["region"],
            "container" => $fileInfo['absPath']['container'],
            "name" => $fileInfo['absPath']['name'],
            "credentials" => [
                "sas_connection_string" => $fileInfo['absCredentials']['SASConnectionString'],
                "expiration" => $fileInfo['absCredentials']['expiration'],
            ],
        ];
    }
}
