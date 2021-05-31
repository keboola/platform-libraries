<?php

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\InputTableOptions;

class Synapse extends AbstractStrategy
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
            /** @var InputTableOptions $table */
            list ($table, $exportOptions) = $export['table'];
            $copyInput = array_merge(
                [
                    'source' => $table->getSource(),
                    'destination' => $table->getDestination(),
                ],
                $exportOptions
            );

            if ($table->isUseView()) {
                $copyInput['useView'] = true;
            }

            $workspaceTables[] = $table;
            $copyInputs[] = $copyInput;
        }
        $workspaceJobs = [];
        $this->logger->info(
            sprintf('Copying %s tables to workspace.', count($copyInputs))
        );
        $job = $this->clientWrapper->getBranchClientIfAvailable()->apiPost(
            'workspaces/' . $this->dataStorage->getWorkspaceId() . '/load',
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
                $manifestPath = $this->ensurePathDelimiter($this->metadataStorage->getPath()) .
                    $this->getDestinationFilePath($this->destination, $table) . ".manifest";
                $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
                $this->manifestCreator->writeTableManifest(
                    $tableInfo,
                    $manifestPath,
                    $table->getColumnNames(),
                    $this->format
                );
            }
        }
    }
}
