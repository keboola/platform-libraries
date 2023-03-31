<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Writer\Table\TableDefinition\TableDefinitionColumnFactory;
use Keboola\StorageApi\Client;
use Keboola\Utils\Sanitizer\ColumnNameSanitizer;

class ColumnsHelper extends AbstractColumnsHelper
{
    public static function addMissingColumns(
        Client $client,
        array $currentTableInfo,
        array $newTableConfiguration
    ): void {
        if ($currentTableInfo['isTyped'] !== false) {
            return;
        }

        $missingColumns = self::getMissingColumns($currentTableInfo, $newTableConfiguration);

        foreach ($newTableConfiguration['column_metadata'] as $columnName => $columnMetadata) {
            $columnName = ColumnNameSanitizer::sanitize($columnName);

            if (!in_array($columnName, $missingColumns)) {
                continue;
            }

            $client->addTableColumn($currentTableInfo['id'], $columnName);
        }
    }
}
