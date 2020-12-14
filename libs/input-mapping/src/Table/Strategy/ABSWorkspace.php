<?php

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\InputTableOptions;

class ABSWorkspace extends AbstractStrategy
{
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
        $workspaceTables = [];

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
            sprintf('Copying %s tables to workspace.', count($copyInputs))
        );
        $job = $this->clientWrapper->getBasicClient()->apiPost(
            'workspaces/' . $this->dataStorage->getWorkspaceId() . '/load',
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
                $manifestPath = $this->metadataStorage->getPath() .
                    $this->getDestinationFilePath($this->destination, $table) . ".manifest";
                $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
                $this->manifestWriter->writeTableManifest($tableInfo, $manifestPath, $table->getColumnNames());
            }
        }
    }
}
