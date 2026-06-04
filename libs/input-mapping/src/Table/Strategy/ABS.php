<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\PathHelper;
use Keboola\StorageApi\Options\GetFileOptions;

class ABS extends AbstractFileStrategy
{
    public function prepareAndExecuteTableLoads(array $tables, bool $preserve): TableLoadQueueInterface
    {
        $this->logger->info('Processing ' . count($tables) . ' ABS table exports.');
        $tablesByJobId = [];
        foreach ($tables as $table) {
            $exportOptions = $table->getStorageApiExportOptions($this->tablesState);
            $exportOptions['gzip'] = true;
            $jobId = $this->clientWrapper->getTableAndFileStorageClient()->queueTableExport(
                $table->getSource(),
                $exportOptions,
            );
            $tablesByJobId[$jobId] = $table;
        }
        return new TableExportQueue($tablesByJobId);
    }

    protected function materializeTableLoads(TableLoadQueueInterface $queue, array $jobResults): void
    {
        if (!$queue instanceof TableExportQueue) {
            throw new InputOperationException('ABS strategy requires TableExportQueue.');
        }

        $keyedResults = [];
        foreach ($jobResults as $result) {
            $keyedResults[$result['id']] = $result;
        }

        foreach ($queue->tablesByJobId as $jobId => $table) {
            $manifestPath = PathHelper::getManifestPath(
                $this->metadataStorage,
                $this->destination,
                $table,
            );
            $tableInfo = $table->getTableInfo();
            $fileInfo = $this->clientWrapper->getTableAndFileStorageClient()->getFile(
                $keyedResults[$jobId]['results']['file']['id'],
                (new GetFileOptions())->setFederationToken(true),
            );
            $tableInfo['abs'] = $this->getABSInfo($fileInfo);
            $this->manifestCreator->writeTableManifest(
                $tableInfo,
                $manifestPath,
                $table->getColumnNamesFromTypes(),
                $this->format,
            );
        }
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
