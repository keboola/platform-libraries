<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhere;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromWorkspace;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;

class TableDataModifier
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
    ) {
    }

    public function updateTableData(MappingFromProcessedConfiguration $source, MappingDestination $destination): void
    {
        foreach ($this->prepareDeleteOptionsList($source) as $deleteOptions) {
            try {
                $this->clientWrapper->getTableAndFileStorageClient()->deleteTableRows(
                    $destination->getTableId(),
                    $deleteOptions,
                );
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    sprintf(
                        'Cannot delete rows from table "%s" in Storage: %s',
                        $destination->getTableId(),
                        $e->getMessage(),
                    ),
                    $e->getCode(),
                    $e,
                );
            }
        }
    }

    private function prepareDeleteOptionsList(MappingFromProcessedConfiguration $source): array
    {
        $deleteWhere = $source->getDeleteWhere();
        if ($deleteWhere !== null) {
            $this->validateDeleteWhereFilters($deleteWhere);
            return array_filter(
                array_map(
                    function (MappingFromConfigurationDeleteWhere $deleteWhere) {
                        return DeleteTableRowsOptionsFactory::createFromDeleteWhere($deleteWhere);
                    },
                    $deleteWhere,
                ),
            );
        }
        if ($source->getDeleteWhereColumn() !== null) {
            return [
                DeleteTableRowsOptionsFactory::createFromLegacyDeleteWhereColumn(
                    $source->getDeleteWhereColumn(),
                    $source->getDeleteWhereOperator(),
                    $source->getDeleteWhereValues(),
                ),
            ];
        }

        return [];
    }

    /**
     * @param MappingFromConfigurationDeleteWhere[] $deleteWhereList
     */
    private function validateDeleteWhereFilters(array $deleteWhereList): void
    {
        foreach ($deleteWhereList as $deleteWhere) {
            if ($deleteWhere->getWhereFilters()) {
                foreach ($deleteWhere->getWhereFilters() as $filter) {
                    if ($filter instanceof MappingFromConfigurationDeleteWhereFilterFromWorkspace) {
                        // Only real branch storage allows 'values_from_workspace' delete filter in development branches
                        if (!$this->clientWrapper->getClientOptionsReadOnly()->useBranchStorage()
                            && $this->clientWrapper->isDevelopmentBranch()) {
                            throw new InvalidOutputException(
                                'Using "values_from_workspace" as a delete filter'
                                    . ' is not supported in development branches.',
                            );
                        }
                    }
                }
            }
        }
    }
}
