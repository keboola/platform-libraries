<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\SearchTablesOptions;
use Psr\Log\LoggerInterface;

/**
 * Class will resolve table 'source' id based on 'source_search' property
 */
class TableDefinitionResolver
{
    public function __construct(
        private readonly Client $storageApiClient,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function resolve(InputTableOptionsList $tablesDefinition): InputTableOptionsList
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

    private function resolveSingleTable(Options\InputTableOptions $table): array
    {
        $tableDefinition = $table->getDefinition();
        $searchSourceConfig = $tableDefinition['source_search'];

        $options = new SearchTablesOptions($searchSourceConfig['key'], $searchSourceConfig['value'], null);
        $tables = $this->storageApiClient->searchTables($options);

        $this->logger->info(sprintf(
            'Resolving table by metadata key: "%s" and value: "%s".',
            $searchSourceConfig['key'],
            $searchSourceConfig['value'],
        ));

        switch (count($tables)) {
            case 0:
                // no table found
                throw new InvalidInputException(sprintf(
                    'Table with metadata key: "%s" and value: "%s" was not found.',
                    $searchSourceConfig['key'],
                    $searchSourceConfig['value'],
                ));
            case 1:
                // one table found
                $this->logger->info(sprintf(
                    'Table with id: "%s" was found.',
                    $tables[0]['id'],
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
            implode(',', $tableNames),
        ));
    }
}
