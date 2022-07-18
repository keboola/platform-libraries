<?php

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StorageApi\Options\GetFileOptions;

class ABS extends AbstractStrategy
{

    public function downloadTable(InputTableOptions $table)
    {
        $exportOptions = $table->getStorageApiExportOptions($this->tablesState);
        $exportOptions['gzip'] = true;
        $jobId = $this->clientWrapper->getBasicClient()->queueTableExport($table->getSource(), $exportOptions);
        return [$jobId, $table];
    }

    public function handleExports($exports, $preserve)
    {
        $this->logger->info("Processing " . count($exports) . " ABS table exports.");
        $jobIds = array_map(function ($export) {
            return $export[0];
        }, $exports);
        $jobResults = $this->clientWrapper->getBasicClient()->handleAsyncTasks($jobIds);
        $keyedResults = [];
        foreach ($jobResults as $result) {
            $keyedResults[$result["id"]] = $result;
        }

        /** @var InputTableOptions $table */
        foreach ($exports as $export) {
            list ($jobId, $table) = $export;
            $manifestPath = $this->ensurePathDelimiter($this->metadataStorage->getPath()) .
                $this->getDestinationFilePath($this->destination, $table) . ".manifest";
            $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
            $fileInfo = $this->clientWrapper->getBasicClient()->getFile(
                $keyedResults[$jobId]["results"]["file"]["id"],
                (new GetFileOptions())->setFederationToken(true)
            )
            ;
            $tableInfo["abs"] = $this->getABSInfo($fileInfo);
            $this->manifestCreator->writeTableManifest(
                $tableInfo,
                $manifestPath,
                $table->getColumnNamesFromTypes(),
                $this->format
            );
        }
        return $jobResults;
    }

    protected function getABSInfo($fileInfo)
    {
        if (empty($fileInfo['absPath'])) {
            throw new InvalidInputException('This project does not have ABS backend.');
        }
        return [
            'is_sliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'container' => $fileInfo['absPath']['container'],
            'name' => $fileInfo['isSliced'] ? $fileInfo['absPath']['name'] . 'manifest' : $fileInfo['absPath']['name'],
            'credentials' => [
                'sas_connection_string' => $fileInfo['absCredentials']['SASConnectionString'],
                'expiration' => $fileInfo['absCredentials']['expiration'],
            ],
        ];
    }
}
