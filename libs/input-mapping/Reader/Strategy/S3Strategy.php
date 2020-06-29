<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\StorageApi\Options\GetFileOptions;

class S3Strategy extends AbstractStrategy
{
    public function downloadTable(InputTableOptions $table)
    {
        $exportOptions = $table->getStorageApiExportOptions($this->tablesState);
        $exportOptions['gzip'] = true;
        $jobId = $this->storageClient->queueTableExport($table->getSource(), $exportOptions);
        return ['jobId' => $jobId, 'table' => $table];
    }

    public function handleExports($exports)
    {
        $this->logger->info("Processing " . count($exports) . " S3 table exports.");
        $jobIds = array_map(function ($export) {
            return $export['jobId'];
        }, $exports);
        $results = $this->storageClient->handleAsyncTasks($jobIds);
        $keyedResults = [];
        foreach ($results as $result) {
            $keyedResults[$result["id"]] = $result;
        }

        /** @var InputTableOptions $table */
        foreach ($exports as $export) {
            $table = $export['table'];
            $manifestPath = $this->getDestinationFilePath($this->destination, $table) . ".manifest";
            $tableInfo = $this->storageClient->getTable($table->getSource());
            $fileInfo = $this->storageClient->getFile(
                $keyedResults[$export['jobId']]["results"]["file"]["id"],
                (new GetFileOptions())->setFederationToken(true)
            )
            ;
            $tableInfo["s3"] = $this->getS3Info($fileInfo);
            $this->manifestWriter->writeTableManifest($tableInfo, $manifestPath, $table->getColumnNames());
        }
    }

    protected function getS3Info($fileInfo)
    {
        return [
            "isSliced" => $fileInfo["isSliced"],
            "region" => $fileInfo["region"],
            "bucket" => $fileInfo["s3Path"]["bucket"],
            "key" => $fileInfo["isSliced"] ? $fileInfo["s3Path"]["key"] . "manifest" : $fileInfo["s3Path"]["key"],
            "credentials" => [
                "access_key_id" => $fileInfo["credentials"]["AccessKeyId"],
                "secret_access_key" => $fileInfo["credentials"]["SecretAccessKey"],
                "session_token" => $fileInfo["credentials"]["SessionToken"]
            ]
        ];
    }
}
