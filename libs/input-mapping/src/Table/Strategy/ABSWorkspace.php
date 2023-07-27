<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StorageApi\Workspaces;

class ABSWorkspace extends AbstractStrategy
{
    public function downloadTable(InputTableOptions $table): array
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
            [$table, $exportOptions] = $export['table'];
            $destination = $this->getDestinationFilePath($this->ensureNoPathDelimiter($this->destination), $table);
            $copyInput = array_merge(
                [
                    'source' => $table->getSource(),
                    'destination' => $this->ensureNoPathDelimiter($destination),
                ],
                $exportOptions
            );

            if ($table->isUseView()) {
                $copyInput['useView'] = true;
            }

            $workspaceTables[] = $table;
            $copyInputs[] = $copyInput;
        }
        $this->logger->info(
            sprintf('Copying %s tables to workspace.', count($copyInputs))
        );
        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
        $workspaceJobId = $workspaces->queueWorkspaceLoadData(
            (int) $this->dataStorage->getWorkspaceId(),
            [
                'input' => $copyInputs,
                'preserve' => 1,
            ]
        );

        $jobResults = [];
        if ($workspaceJobId) {
            $this->logger->info('Processing workspace export.');
            $jobResults = $this->clientWrapper->getBranchClientIfAvailable()->handleAsyncTasks([$workspaceJobId]);
            foreach ($workspaceTables as $table) {
                $manifestPath = $this->ensurePathDelimiter($this->metadataStorage->getPath()) .
                    $this->getDestinationFilePath($this->ensureNoPathDelimiter($this->destination), $table) .
                    '.manifest';
                $tableInfo = $table->getTableInfo();
                $this->manifestCreator->writeTableManifest(
                    $tableInfo,
                    $manifestPath,
                    $table->getColumnNamesFromTypes(),
                    $this->format
                );
            }
        }
        return $jobResults;
    }
}
