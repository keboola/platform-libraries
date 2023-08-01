<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\StorageApi\Workspaces;

class Synapse extends AbstractStrategy
{
    public function downloadTable(RewrittenInputTableOptions $table): array
    {
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        $this->logger->info(sprintf('Table "%s" will be copied.', $table->getSource()));
        return [
            'table' => [$table, $loadOptions],
            'type' => 'copy',
        ];
    }

    public function handleExports(array $exports, bool $preserve): array
    {
        $copyInputs = [];
        $workspaceTables = [];

        foreach ($exports as $export) {
            /** @var RewrittenInputTableOptions $table */
            [$table, $exportOptions] = $export['table'];
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
        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());

        $workspaceJobs[] = $workspaces->queueWorkspaceLoadData(
            (int) $this->dataStorage->getWorkspaceId(),
            [
                'input' => $copyInputs,
                'preserve' => $preserve,
            ]
        );

        $this->logger->info('Processing ' . count($workspaceJobs) . ' workspace exports.');
        $jobResults = $this->clientWrapper->getBranchClient()->handleAsyncTasks($workspaceJobs);
        foreach ($workspaceTables as $table) {
            $manifestPath = $this->ensurePathDelimiter($this->metadataStorage->getPath()) .
                $this->getDestinationFilePath($this->destination, $table) . '.manifest';
            $tableInfo = $table->getTableInfo();
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
