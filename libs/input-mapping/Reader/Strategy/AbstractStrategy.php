<?php

namespace Keboola\InputMapping\Reader\Strategy;

use Keboola\InputMapping\Reader\ManifestWriter;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApi\Client;
use Psr\Log\LoggerInterface;

abstract class AbstractStrategy implements StrategyInterface
{
    /** @var Client */
    protected $storageClient;

    /** LoggerInterface */
    protected $logger;

    /** @var WorkspaceProviderInterface */
    protected $workspaceProvider;

    /** @var InputTableStateList */
    protected $tablesState;

    /** string */
    protected $destination;

    /** @var ManifestWriter */
    protected $manifestWriter;

    public function __construct(
        Client $storageClient,
        LoggerInterface $logger,
        WorkspaceProviderInterface $workspaceProvider,
        InputTableStateList $tablesState,
        $destination,
        $format = 'json'
    ) {
        $this->storageClient = $storageClient;
        $this->logger = $logger;
        $this->workspaceProvider = $workspaceProvider;
        $this->tablesState = $tablesState;
        $this->destination = $destination;
        $this->manifestWriter = new ManifestWriter($this->storageClient, $format);
    }

    /**
     * @param InputTableOptions[]
     * @return InputTableStateList
     */
    public function downloadTables($tables)
    {
        $outputStateConfiguration = [];
        $exports = [];
        /** @var InputTableOptions $table */
        foreach ($tables as $table) {
            $tableInfo = $this->storageClient->getTable($table->getSource());
            $outputStateConfiguration[] = [
                'source' => $table->getSource(),
                'lastImportDate' => $tableInfo['lastImportDate']
            ];
            $exports[] = $this->downloadTable($table);
            $this->logger->info("Fetched table " . $table->getSource() . ".");
        }

        $this->handleExports($exports);

        $this->logger->info("All tables were fetched.");

        return new InputTableStateList($outputStateConfiguration);
    }

    /**
     * @param string $destination
     * @param InputTableOptions $table
     * @return string
     */
    protected function getDestinationFilePath($destination, InputTableOptions $table)
    {
        if (!$table->getDestination()) {
            return $destination . "/" . $table->getSource();
        } else {
            return $destination . "/" . $table->getDestination();
        }
    }
}
