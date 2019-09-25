<?php

namespace Keboola\InputMapping\Reader;

use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\SearchTablesOptions;

class TableDefinitionResolver
{
    /**
     * @var Client
     */
    private $storageApiClient;

    public function __construct(Client $client)
    {
        $this->storageApiClient = $client;
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
     * @throws ClientException
     */
    private function resolveSingleTable(Options\InputTableOptions $table)
    {
        $tableDefinition = $table->getDefinition();
        $searchSourceConfig = $tableDefinition['search_source'];

        $options = SearchTablesOptions::create($searchSourceConfig['key'], $searchSourceConfig['value'], null);
        $tables = $this->storageApiClient->searchTables($options);

        if (0 === count($tables)) {
            throw new \Exception(sprintf(
                'Table with metadata key: %s and value: %s was not found',
                $searchSourceConfig['key'],
                $searchSourceConfig['value']
            ));
        }
        if (1 !== count($tables)) {
            $tableNames = array_map(function ($t) {
                return $t['id'];
            }, $tables);
            throw new \Exception(sprintf(
                'More than one table with metadata key: %s and value: %s was not found: %s',
                $searchSourceConfig['key'],
                $searchSourceConfig['value'],
                implode(',', $tableNames)
            ));
        }

        $tableDefinition['source'] = $tables[0]['id'];

        return $tableDefinition;
    }
}
