<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\PathHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;

class Local extends AbstractFileStrategy
{
    public const DEFAULT_MAX_EXPORT_SIZE_BYTES = 100000000000;
    public const EXPORT_SIZE_LIMIT_NAME = 'components.max_export_size_bytes';

    public function prepareAndExecuteTableLoads(array $tables, bool $preserve): TableLoadQueueInterface
    {
        $tokenInfo = $this->clientWrapper->getBranchClient()->verifyToken();
        $exportLimit = self::DEFAULT_MAX_EXPORT_SIZE_BYTES;
        if (!empty($tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME])) {
            $exportLimit = $tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME]['value'];
        }

        $this->logger->info('Processing ' . count($tables) . ' local table exports.');
        $tableExporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());

        $tablesByJobId = [];
        $exportJobs = [];
        foreach ($tables as $table) {
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

            $queuedJobs = $tableExporter->queueTableExports([
                [
                    'tableId' => $table->getSource(),
                    'destination' => PathHelper::getDataFilePath($this->dataStorage, $this->destination, $table),
                    'exportOptions' => $table->getStorageApiExportOptions($this->tablesState),
                ],
            ]);
            $jobId = array_key_first($queuedJobs);
            assert($jobId !== null);
            $tablesByJobId[$jobId] = $table;
            $exportJobs[$jobId] = $queuedJobs[$jobId];
        }

        return new TableExportQueue($tablesByJobId, $exportJobs);
    }

    protected function materializeTableLoads(TableLoadQueueInterface $queue, array $jobResults): void
    {
        if (!$queue instanceof TableExportQueue) {
            throw new InputOperationException('Local strategy requires TableExportQueue.');
        }

        $tableExporter = new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
        $tableExporter->downloadExportedFiles($jobResults, $queue->exportJobs);

        foreach ($queue->getAllTables() as $table) {
            $this->manifestCreator->writeTableManifest(
                $table->getTableInfo(),
                PathHelper::getManifestPath($this->metadataStorage, $this->destination, $table),
                $table->getColumnNamesFromTypes(),
                $this->format,
            );
        }
    }

    protected function getAwaitingClient(): Client
    {
        return $this->clientWrapper->getTableAndFileStorageClient();
    }
}
