<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\PathHelper;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\StorageApi\Options\GetFileOptions;

class ABS extends AbstractFileStrategy
{
    public function downloadTable(RewrittenInputTableOptions $table): array
    {
        $exportOptions = $table->getStorageApiExportOptions($this->tablesState);
        $exportOptions['gzip'] = true;
        $jobId = $this->clientWrapper->getTableAndFileStorageClient()->queueTableExport(
            $table->getSource(),
            $exportOptions,
        );
        return [$jobId, $table];
    }

    public function handleExports(array $exports, bool $preserve): array
    {
        $this->logger->info('Processing ' . count($exports) . ' ABS table exports.');
        $jobIds = array_map(function ($export) {
            return $export[0];
        }, $exports);
        $jobResults = $this->clientWrapper->getBranchClient()->handleAsyncTasks($jobIds);
        $keyedResults = [];
        foreach ($jobResults as $result) {
            $keyedResults[$result['id']] = $result;
        }

        foreach ($exports as $export) {
            /** @var RewrittenInputTableOptions $table */
            [$jobId, $table] = $export;
            $manifestPath = PathHelper::getManifestPath(
                $this->metadataStorage,
                $this->destination,
                $table,
            );
            $tableInfo = $table->getTableInfo();
            $fileInfo = $this->clientWrapper->getTableAndFileStorageClient()->getFile(
                $keyedResults[$jobId]['results']['file']['id'],
                (new GetFileOptions())->setFederationToken(true),
            )
            ;
            $tableInfo['abs'] = $this->getABSInfo($fileInfo);
            $this->manifestCreator->writeTableManifest(
                $tableInfo,
                $manifestPath,
                $table->getColumnNamesFromTypes(),
                $this->format,
            );
        }
        return $jobResults;
    }

    protected function getABSInfo(array $fileInfo): array
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
