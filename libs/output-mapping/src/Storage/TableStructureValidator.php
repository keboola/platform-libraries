<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

use Keboola\Datatype\Definition\BaseType;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\InvalidTableStructureException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaPrimaryKey;
use Keboola\OutputMapping\Writer\Helper\PrimaryKeyHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\LoggerInterface;

class TableStructureValidator extends AbstractTableStructureValidator
{
    public function validate(?array $schemaColumns): TableChangesStore
    {
        if ($this->table['isTyped'] === true) {
            throw new InvalidOutputException(sprintf('Table "%s" is typed.', $this->table['id']));
        }

        $tableChangesStore = new TableChangesStore();

        if (is_null($schemaColumns)) {
            return $tableChangesStore;
        }

        $tableChangesStore = $this->validateColumnsName(
            $this->table['id'],
            $this->table['columns'],
            $schemaColumns,
            $tableChangesStore,
        );

        return $this->validatePrimaryKeys(
            $this->table['primaryKey'],
            $schemaColumns,
            $tableChangesStore,
        );
    }
}
