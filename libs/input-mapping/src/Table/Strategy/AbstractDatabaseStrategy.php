<?php

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Helper\LoadTypeDecider;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StorageApi\Workspaces;

abstract class AbstractDatabaseStrategy extends AbstractStrategy
{
    /** @return string */
    abstract protected function getWorkspaceType();

    public function downloadTable(InputTableOptions $table)
    {
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        if (LoadTypeDecider::canClone($tableInfo, $this->getWorkspaceType(), $loadOptions)) {
            $this->logger->info(sprintf('Table "%s" will be cloned.', $table->getSource()));
            return [
                'table' => $table,
                'type' => 'clone'
            ];
        }
        $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
        return [
            'table' => [$table, $loadOptions],
            'type' => 'copy',
        ];
    }

    public function handleExports($exports, $preserve)
    {
        $cloneInputs = [];
        $copyInputs = [];
        $workspaceTables = [];

        foreach ($exports as $export) {
            if ($export['type'] === 'clone') {
                /** @var InputTableOptions $table */
                $table = $export['table'];
                $cloneInputs[] = [
                    'source' => $table->getSource(),
                    'destination' => $table->getDestination(),
                    'overwrite' => $table->getOverwrite(),
                ];
                $workspaceTables[] = $table;
            }
            if ($export['type'] === 'copy') {
                [$table, $exportOptions] = $export['table'];
                $copyInput = array_merge(
                    [
                        'source' => $table->getSource(),
                        'destination' => $table->getDestination()
                    ],
                    $exportOptions
                );

                if ($table->isUseView()) {
                    $copyInput['useView'] = true;
                }

                $workspaceTables[] = $table;
                $copyInputs[] = $copyInput;
            }
        }

        $cloneJobResult = [];
        $copyJobResult = [];
        $hasBeenCleaned = false;

        $workspaces = new Workspaces($this->clientWrapper->getBranchClientIfAvailable());

        if ($cloneInputs) {
            $this->logger->info(
                sprintf('Cloning %s tables to workspace.', count($cloneInputs))
            );
            // here we are waiting for the jobs to finish. handleAsyncTask = true
            // We need to process clone and copy jobs separately because there is no lock on the table and there is a race between the
            // clone and copy jobs which can end in an error that the table already exists.
            // Full description of the issue here: https://keboola.atlassian.net/wiki/spaces/KB/pages/2383511594/Input+mapping+to+workspace+Consolidation#Context
            $jobId = $workspaces->queueWorkspaceCloneInto(
                $this->dataStorage->getWorkspaceId(),
                [
                    'input' => $cloneInputs,
                    'preserve' => $preserve ? 1 : 0,
                ]
            );
            $cloneJobResult = $this->clientWrapper->getBasicClient()->handleAsyncTasks([$jobId]);
            if (!$preserve) {
                $hasBeenCleaned = true;
            }
        }

        if ($copyInputs) {
            $this->logger->info(
                sprintf('Copying %s tables to workspace.', count($copyInputs))
            );
            $jobId = $workspaces->queueWorkspaceLoadData(
                $this->dataStorage->getWorkspaceId(),
                [
                    'input' => $copyInputs,
                    'preserve' => !$hasBeenCleaned && !$preserve ? 0 : 1,
                ]
            );
            $copyJobResult = $this->clientWrapper->getBasicClient()->handleAsyncTasks([$jobId]);
        }
        $jobResults = array_merge($cloneJobResult, $copyJobResult);
        $this->logger->info('Processed ' . count($jobResults) . ' workspace exports.');

        foreach ($workspaceTables as $table) {
            $manifestPath = $this->ensurePathDelimiter($this->metadataStorage->getPath()) .
                $this->getDestinationFilePath($this->destination, $table) . ".manifest";
            $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
            $this->manifestCreator->writeTableManifest(
                $tableInfo,
                $manifestPath,
                $table->getColumnNamesFromTypes(),
                $this->format
            );
        }
        return $jobResults;
    }
}
