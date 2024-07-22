<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Psr\Log\LoggerInterface;

class PrimaryKeyHelper
{
    public static function normalizeKeyArray(LoggerInterface $logger, array $keys): array
    {
        return array_map(
            function ($key) {
                return trim($key);
            },
            array_unique(
                array_filter($keys, function ($col) use ($logger) {
                    if ($col !== '') {
                        return true;
                    }
                    $logger->warning('Found empty column name in key array.');
                    return false;
                }),
            ),
        );
    }

    public static function modifyPrimaryKeyDecider(
        LoggerInterface $logger,
        array $currentTablePrimaryKey,
        array $newTableConfigurationPrimaryKey,
    ): bool {
        $configPK = self::normalizeKeyArray($logger, $newTableConfigurationPrimaryKey);
        if (count($currentTablePrimaryKey) !== count($configPK)) {
            return true;
        }
        $currentTablePkColumnsCount = count($currentTablePrimaryKey);
        if (count(array_intersect($currentTablePrimaryKey, $configPK)) !== $currentTablePkColumnsCount) {
            return true;
        }
        return false;
    }
}
