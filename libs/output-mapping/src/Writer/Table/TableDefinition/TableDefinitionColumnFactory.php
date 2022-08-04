<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\Datatype\Definition\Common;

class TableDefinitionColumnFactory
{
    public function createTableDefinitionColumn($columnName, $metadata): TableDefinitionColumn
    {
        $baseType = $this->getBaseTypeFromMetadata($metadata);
        return new TableDefinitionColumn($columnName, $baseType);
    }

    private function getBaseTypeFromMetadata(array $metadata): ?string
    {
        foreach ($metadata as $metadatum) {
            if ($metadatum['key'] === Common::KBC_METADATA_KEY_BASETYPE) {
                return $metadatum['value'];
            }
        }
        return null;
    }
}
