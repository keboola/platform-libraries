<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\StorageApi\Client;

class TableColumnsHelper
{
    public static function addMissingColumns(
        Client $client,
        array $currentTableInfo,
        array $newTableConfiguration,
        string $backendType,
    ): void {
        if ($currentTableInfo['isTyped'] !== true) {
            TypedColumnsHelper::addMissingColumns($client, $currentTableInfo, $newTableConfiguration, $backendType);
        } else {
            ColumnsHelper::addMissingColumns($client, $currentTableInfo, $newTableConfiguration);
        }
    }
}
