<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Reader\LoadTypeDecider;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;

class ABSWorkspaceStrategy extends AbstractStrategy
{
    protected $workspaceProviderId = WorkspaceProviderInterface::TYPE_ABS;

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
        $copyInputs = [];

        foreach ($exports as $export) {
            list ($table, $exportOptions) = $export['table'];
            $destination = $this->getDestinationFilePath($this->destination, $table);
            $copyInputs[] = array_merge(
                [
                    'source' => $table->getSource(),
                    'destination' => $destination,
                ],
                $exportOptions
            );
            $workspaceTables[] = $table;
        }
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
        $workspaceJobId = $job['id'];

        if ($workspaceJobId) {
            $this->logger->info('Processing workspace export.');
            $this->clientWrapper->getBasicClient()->handleAsyncTasks([$workspaceJobId]);
            foreach ($workspaceTables as $table) {
                $manifestPath = $this->getDestinationFilePath($this->destination, $table) . ".manifest";
                $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
                $this->manifestWriter->writeTableManifest($tableInfo, $manifestPath, $table->getColumnNames());
            }
        }
    }
}
