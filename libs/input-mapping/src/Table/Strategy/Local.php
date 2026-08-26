<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\LogFormatter;
use Keboola\InputMapping\Helper\PathHelper;
use Keboola\InputMapping\Helper\Timer;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;

class Local extends AbstractFileStrategy
{
    public const DEFAULT_MAX_EXPORT_SIZE_BYTES = 100000000000;
    public const EXPORT_SIZE_LIMIT_NAME = 'components.max_export_size_bytes';

    public function prepareAndExecuteTableLoads(array $tables, bool $preserve): TableLoadQueueInterface
    {
        $timer = Timer::start();
        $tokenInfo = $this->clientWrapper->getBranchClient()->verifyToken();
        $exportLimit = self::DEFAULT_MAX_EXPORT_SIZE_BYTES;
        if (!empty($tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME])) {
            $exportLimit = $tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME]['value'];
        }

        $this->logger->info('Processing ' . count($tables) . ' local table exports.');
        $tableExporter = $this->createTableExporter();

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

        $this->logger->debug(sprintf(
            'Queued %s table exports in %s.',
            count($tablesByJobId),
            LogFormatter::formatDuration($timer->getElapsedSeconds()),
        ));

        return new TableExportQueue($tablesByJobId, static::class, $this->destination, $exportJobs);
    }

    protected function materializeTableLoads(TableLoadQueueInterface $queue, array $jobResults): void
    {
        if (!$queue instanceof TableExportQueue) {
            throw new InputOperationException('Local strategy requires TableExportQueue.');
        }

        $this->downloadTableExports($queue, $jobResults);
        $this->writeManifests($queue);
    }

    /**
     * Downloads one export at a time so that each table gets its own timing.
     *
     * TableExporter::downloadExportedFiles() is a plain loop over the job results, so N single-result
     * calls do exactly what one N-result call did before.
     *
     * @param array $jobResults results of the finished Storage jobs, as returned by handleAsyncTasks()
     */
    private function downloadTableExports(TableExportQueue $queue, array $jobResults): void
    {
        $resultsByJobId = [];
        foreach ($jobResults as $jobResult) {
            $resultsByJobId[$jobResult['id']] = $jobResult;
        }

        $tableExporter = $this->createTableExporter();
        $tableCount = count($queue->tablesByJobId);
        $this->logger->debug(sprintf('Downloading %s exported tables.', $tableCount));

        $phaseTimer = Timer::start();
        $totalBytes = 0;

        foreach ($queue->tablesByJobId as $jobId => $table) {
            if (!isset($resultsByJobId[$jobId])) {
                throw new InputOperationException(sprintf(
                    'Result of export job "%s" for table "%s" is missing.',
                    $jobId,
                    $table->getSource(),
                ));
            }
            $jobResult = $resultsByJobId[$jobId];
            $fileId = $jobResult['results']['file']['id'] ?? 'unknown';
            $bytes = (int) ($jobResult['metrics']['outBytes'] ?? 0);
            $totalBytes += $bytes;

            $this->logger->debug(sprintf(
                'Fetching table %s (export job %s, file %s).',
                $table->getSource(),
                $jobId,
                $fileId,
            ));

            $tableTimer = Timer::start();
            $tableExporter->downloadExportedFiles([$jobResult], $queue->exportJobs);
            $tableSeconds = $tableTimer->getElapsedSeconds();

            $this->logTableFetched($table->getSource(), sprintf(
                'Downloaded %s in %s (%s), export job %s, file %s.',
                LogFormatter::formatBytes($bytes),
                LogFormatter::formatDuration($tableSeconds),
                LogFormatter::formatThroughput($bytes, $tableSeconds),
                $jobId,
                $fileId,
            ));
        }

        $phaseSeconds = $phaseTimer->getElapsedSeconds();
        $this->logger->debug(sprintf(
            'Downloaded %s tables, %s in %s (%s).',
            $tableCount,
            LogFormatter::formatBytes($totalBytes),
            LogFormatter::formatDuration($phaseSeconds),
            LogFormatter::formatThroughput($totalBytes, $phaseSeconds),
        ));
    }

    private function writeManifests(TableExportQueue $queue): void
    {
        $timer = Timer::start();
        $tables = $queue->getAllTables();
        foreach ($tables as $table) {
            $this->manifestCreator->writeTableManifest(
                $table->getTableInfo(),
                PathHelper::getManifestPath($this->metadataStorage, $this->destination, $table),
                $table->getColumnNamesFromTypes(),
                $this->format,
            );
        }

        $this->logManifestsWritten(count($tables), $timer->getElapsedSeconds());
    }

    protected function createTableExporter(): TableExporter
    {
        return new TableExporter($this->clientWrapper->getTableAndFileStorageClient());
    }

    protected function getAwaitingClient(): Client
    {
        return $this->clientWrapper->getTableAndFileStorageClient();
    }
}
