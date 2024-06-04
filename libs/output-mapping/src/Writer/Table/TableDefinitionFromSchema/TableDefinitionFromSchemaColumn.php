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

        $dataType = $this->column->getDataType();
        if ($dataType) {
            $data['basetype'] = $dataType->getBaseTypeName();

            $definition = [];
            if ($dataType->getBaseTypeName() !== $dataType->getTypeName($this->backend)) {
                $definition['type'] = $dataType->getTypeName($this->backend);
            }
            if ($dataType->getLength($this->backend) !== null) {
                $definition['length'] = $dataType->getLength($this->backend);
            }
            if (!$this->column->isNullable()) {
                $definition['nullable'] = false;
            }
            if ($dataType->getDefaultValue($this->backend) !== null) {
                $definition['default'] = $dataType->getDefaultValue($this->backend);
            }

            if ($definition && !isset($definition['type'])) {
                $definition['type'] = $dataType->getTypeName($this->backend);
            }
            if ($definition) {
                $data['definition'] = $definition;
            }
        }

        return $data;
    }
}
