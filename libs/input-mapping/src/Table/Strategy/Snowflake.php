<?php

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Helper\LoadTypeDecider;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\WorkspaceProviderInterface;

class Snowflake extends AbstractStrategy
{
    protected $workspaceProviderId = WorkspaceProviderInterface::TYPE_SNOWFLAKE;

    public function downloadTable(InputTableOptions $table)
    {
        $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
        $loadOptions = $table->getStorageApiLoadOptions($this->tablesState);
        if (LoadTypeDecider::canClone($tableInfo, 'snowflake', $loadOptions)) {
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
                    'destination' => $table->getDestination(),
                    'overwrite' => $table->getOverwrite(),
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

        $workspaceJobs = [];
        if ($cloneInputs) {
            $this->logger->info(
                sprintf('Cloning %s tables to %s workspace.', count($cloneInputs), $this->workspaceProviderId)
            );
            $job = $this->clientWrapper->getBasicClient()->apiPost(
                'workspaces/' . $this->workspaceProvider->getWorkspaceId($this->workspaceProviderId) . '/load-clone',
                [
                    'input' => $cloneInputs,
                    'preserve' => 1,
                ],
                false
            );
            $workspaceJobs[] = $job['id'];
        }

        if ($copyInputs) {
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
        }

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
