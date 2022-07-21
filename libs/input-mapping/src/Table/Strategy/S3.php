<?php

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StorageApi\Options\GetFileOptions;

class S3 extends AbstractStrategy
{
    public function downloadTable(InputTableOptions $table)
    {
        $exportOptions = $table->getStorageApiExportOptions($this->tablesState);
        $exportOptions['gzip'] = true;
        $jobId = $this->clientWrapper->getBasicClient()->queueTableExport($table->getSource(), $exportOptions);
        return ['jobId' => $jobId, 'table' => $table];
    }

    public function handleExports($exports, $preserve)
    {
        $this->logger->info("Processing " . count($exports) . " S3 table exports.");
        $jobIds = array_map(function ($export) {
            return $export['jobId'];
        }, $exports);
        $jobResults = $this->clientWrapper->getBasicClient()->handleAsyncTasks($jobIds);
        $keyedResults = [];
        foreach ($jobResults as $result) {
            $keyedResults[$result["id"]] = $result;
        }

        /** @var InputTableOptions $table */
        foreach ($exports as $export) {
            $table = $export['table'];
            $manifestPath = $this->ensurePathDelimiter($this->metadataStorage->getPath()) .
                $this->getDestinationFilePath($this->destination, $table) . ".manifest";
            $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
            $fileInfo = $this->clientWrapper->getBasicClient()->getFile(
                $keyedResults[$export['jobId']]["results"]["file"]["id"],
                (new GetFileOptions())->setFederationToken(true)
            )
            ;
            $tableInfo["s3"] = $this->getS3Info($fileInfo);
            $this->manifestCreator->writeTableManifest(
                $tableInfo,
                $manifestPath,
                $table->getColumnNamesFromTypes(),
                $this->format
            );
        }
        return $jobResults;
    }

    protected function getS3Info($fileInfo)
    {
        if (empty($fileInfo["credentials"]["AccessKeyId"])) {
            throw new InvalidInputException('This project does not have S3 backend.');
        }
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
