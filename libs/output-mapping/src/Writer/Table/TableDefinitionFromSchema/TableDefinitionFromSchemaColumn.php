<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;

class TableDefinitionFromSchemaColumn
{
    public function __construct(
        readonly private MappingFromConfigurationSchemaColumn $column,
        readonly private string $backend,
    ) {
    }

    public function getRequestData(): array
    {
        $data = [
            'name' => $this->column->getName(),
        ];

        $definition = [];

        $dataType = $this->column->getDataType();
        if ($dataType) {
            $data['basetype'] = $dataType->getBaseTypeName();

            if ($this->backend !== null && $dataType->hasBackendType($this->backend)) {
                $definition['type'] = $dataType->getBackendTypeName($this->backend);
            }
            if ($dataType->getLength($this->backend) !== null) {
                $definition['length'] = $dataType->getLength($this->backend);
            }
            if (!$this->column->isNullable()) {
                $definition['nullable'] = false;
            }
            // Default value is not works correctly
            // if ($dataType->getDefaultValue($this->backend) !== null) {
            //    $definition['default'] = $dataType->getDefaultValue($this->backend);
            // }

            if ($definition && !isset($definition['type'])) {
                $definition['type'] = $dataType->getTypeName($this->backend);
            }
        }

        // The create-table-definition endpoint stores the column description inside the column `definition`
        // (definition.description), unlike the update endpoint which uses a top-level `description`. A
        // definition without a type may carry only a description, so this is valid even when the column has
        // no data type.
        if ($this->column->getDescription() !== null) {
            $definition['description'] = $this->column->getDescription();
        }

        if ($definition) {
            $data['definition'] = $definition;
        }

        return $data;
    }
}
