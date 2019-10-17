<?php

namespace Keboola\InputMapping\Reader;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\SearchTablesOptions;
use Psr\Log\LoggerInterface;

/**
 * Class will resolve table 'source' id based on 'source_search' property
 */
class TableDefinitionResolver
{
    /**
     * @var Client
     */
    private $storageApiClient;

    /**
     * @var LoggerInterface
     */
    private $logger;

    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->storageApiClient = $client;
        $this->logger = $logger;
    }

    /**
     * @param InputTableOptionsList $tablesDefinition
     * @return InputTableOptionsList
     */
    public function resolve(InputTableOptionsList $tablesDefinition)
    {
        $resolvedTables = [];
        foreach ($tablesDefinition->getTables() as $table) {
            if (!empty($table->getDefinition()['source'])) {
                // if source is set there is no need to resolve table
                $resolvedTables[] = $table->getDefinition();
                continue;
            }
            $resolvedTables[] = $this->resolveSingleTable($table);
        }
        return new InputTableOptionsList($resolvedTables);
    }

    /**
     * @param Options\InputTableOptions $table
     * @return array
     * @throws InvalidInputException
     */
    private function resolveSingleTable(Options\InputTableOptions $table)
    {
        $tableDefinition = $table->getDefinition();
        $searchSourceConfig = $tableDefinition['source_search'];

        $options = new SearchTablesOptions($searchSourceConfig['key'], $searchSourceConfig['value'], null);
        $tables = $this->storageApiClient->searchTables($options);

        $this->logger->info(sprintf(
            'Resolving table by metadata key: "%s" and value: "%s".',
            $searchSourceConfig['key'],
            $searchSourceConfig['value']
        ));

        switch (count($tables)) {
            case 0:
                // no table found
                throw new InvalidInputException(sprintf(
                    'Table with metadata key: "%s" and value: "%s" was not found.',
                    $searchSourceConfig['key'],
                    $searchSourceConfig['value']
                ));
                break;
            case 1:
                // one table found
                $this->logger->info(sprintf(
                    'Table with id: "%s" was found.',
                    $tables[0]['id']
                ));

                $tableDefinition['source'] = $tables[0]['id'];

                return $tableDefinition;
        }

        // more than one table found

        $tableNames = array_map(function ($t) {
            return $t['id'];
        }, $tables);

        throw new InvalidInputException(sprintf(
            'More than one table with metadata key: "%s" and value: "%s" was found: %s.',
            $searchSourceConfig['key'],
            $searchSourceConfig['value'],
            implode(',', $tableNames)
        ));
    }
}
