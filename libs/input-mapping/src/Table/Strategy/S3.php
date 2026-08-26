<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\LogFormatter;
use Keboola\InputMapping\Helper\PathHelper;
use Keboola\InputMapping\Helper\Timer;
use Keboola\StorageApi\Options\GetFileOptions;

class S3 extends AbstractFileStrategy
{
    public function prepareAndExecuteTableLoads(array $tables, bool $preserve): TableLoadQueueInterface
    {
        $timer = Timer::start();
        $this->logger->info('Processing ' . count($tables) . ' S3 table exports.');
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

        $this->logger->debug(sprintf(
            'Queued %s table exports in %s.',
            count($tablesByJobId),
            LogFormatter::formatDuration($timer->getElapsedSeconds()),
        ));

        return new TableExportQueue($tablesByJobId, static::class, $this->destination);
    }

    protected function materializeTableLoads(TableLoadQueueInterface $queue, array $jobResults): void
    {
        if (!$queue instanceof TableExportQueue) {
            throw new InputOperationException('S3 strategy requires TableExportQueue.');
        }

        $keyedResults = [];
        foreach ($jobResults as $result) {
            $keyedResults[$result['id']] = $result;
        }

        $timer = Timer::start();
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
            $tableInfo['s3'] = $this->getS3Info($fileInfo);
            $this->manifestCreator->writeTableManifest(
                $tableInfo,
                $manifestPath,
                $table->getColumnNamesFromTypes(),
                $this->format,
            );
            $this->logTableFetched($table->getSource(), sprintf(
                'Export job %s, file %s.',
                $jobId,
                $fileInfo['id'],
            ));
        }
        $this->logManifestsWritten(count($queue->tablesByJobId), $timer->getElapsedSeconds());
    }

    /**
     * @return array {
     *      isSliced: bool,
     *      region: string,
     *      bucket: string,
     *      key: string,
     *      credentials: array {
     *          access_key_id: string,
     *          secret_access_key: string,
     *          session_token: string,
     *      }
     * }
     */
    protected function getS3Info(array $fileInfo): array
    {
        if (empty($fileInfo['credentials']['AccessKeyId'])) {
            throw new InvalidInputException('This project does not have S3 backend.');
        }
        return [
            'isSliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'bucket' => $fileInfo['s3Path']['bucket'],
            'key' => $fileInfo['isSliced'] ? $fileInfo['s3Path']['key'] . 'manifest' : $fileInfo['s3Path']['key'],
            'credentials' => [
                'access_key_id' => $fileInfo['credentials']['AccessKeyId'],
                'secret_access_key' => $fileInfo['credentials']['SecretAccessKey'],
                'session_token' => $fileInfo['credentials']['SessionToken'],
            ],
        ];
    }
}
