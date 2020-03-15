<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Reader\LoadTypeDecider;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;

class RedshiftStrategy extends AbstractStrategy
{
    protected $workspaceProviderId = WorkspaceProviderInterface::TYPE_REDSHIFT;

    public function downloadTable(InputTableOptions $table)
    {
        $tableInfo = $this->storageClient->getTable($table->getSource());
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        if (LoadTypeDecider::canClone($tableInfo, 'redshift', $loadOptions)) {
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

    public function handleExports($exports)
    {
        $cloneInputs = [];
        $copyInputs = [];

        foreach ($exports as $export) {
            if ($export['type'] === 'clone') {
                /** @var InputTableOptions $table */
                $table = $export['table'];
                $cloneInputs[] = [
                    'source' => $table->getSource(),
                    'destination' => $table->getDestination()
                ];
                $workspaceTables[] = $table;
            }
            if ($export['type'] === 'copy') {
                list ($table, $exportOptions) = $export['table'];
                $copyInputs[] = array_merge(
                    [
                        'source' => $table->getSource(),
                        'destination' => $table->getDestination(),
                    ],
                    $exportOptions
                );
                $workspaceTables[] = $table;
            }
        }

        $this->logger->info(
            sprintf('Cloning %s tables to %s workspace.', count($cloneInputs), $this->workspaceProviderId)
        );
        $job = $this->storageClient->apiPost(
            'storage/workspaces/' . $this->workspaceProvider->getWorkspaceId($this->workspaceProviderId) . '/load-clone',
            [
                'input' => $cloneInputs,
                'preserve' => 1,
            ],
            false
        );
        $workspaceJobs[] = $job['id'];

        $this->logger->info(
            sprintf('Copying %s tables to %s workspace.', count($copyInputs), $this->workspaceProviderId)
        );
        $job = $this->storageClient->apiPost(
            'storage/workspaces/' . $this->workspaceProvider->getWorkspaceId($this->workspaceProviderId) . '/load',
            [
                'input' => $copyInputs,
                'preserve' => 1,
            ],
            false
        );
        $workspaceJobs[] = $job['id'];


        $workspaceJobs = [];
        $workspaceTables = [];

        if ($workspaceJobs) {
            $this->logger->info('Processing ' . count($workspaceJobs) . ' workspace exports.');
            $this->storageClient->handleAsyncTasks($workspaceJobs);
            foreach ($workspaceTables as $table) {
                $manifestPath = $this->getDestinationFilePath($this->destination, $table) . ".manifest";
                $tableInfo = $this->storageClient->getTable($table->getSource());
                $this->manifestWriter->writeTableManifest($tableInfo, $manifestPath, $table->getColumns());
            }
        }
    }
}
