<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;

class SynapseStrategy extends AbstractStrategy
{

    protected $workspaceProviderId = WorkspaceProviderInterface::TYPE_SYNAPSE;

    public function downloadTable(InputTableOptions $table)
    {
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
        return [
            'table' => [$table, $loadOptions],
            'type' => 'copy',
        ];
    }

    public function handleExports($exports)
    {
        foreach ($exports as $export) {
            /** @var InputTableOptions $table */
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
        $workspaceJobs = [];
        $this->logger->info(
            sprintf('Copying %s tables to %s workspace.', count($copyInputs), $this->workspaceProviderId)
        );
        $job = $this->clientWrapper->getBasicClient()->apiPost(
            'workspaces/' . $this->workspaceProvider->getWorkspaceId($this->workspaceProviderId) . '/load',
            [
                'input' => $copyInputs,
                'preserve' => 1,
            ],
            false
        );
        $workspaceJobs[] = $job['id'];

        if ($workspaceJobs) {
            $this->logger->info('Processing ' . count($workspaceJobs) . ' workspace exports.');
            $this->clientWrapper->getBasicClient()->handleAsyncTasks($workspaceJobs);
            foreach ($workspaceTables as $table) {
                $manifestPath = $this->getDestinationFilePath($this->destination, $table) . ".manifest";
                $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
                $this->manifestWriter->writeTableManifest($tableInfo, $manifestPath, $table->getColumnNames());
            }
        }
    }
}
