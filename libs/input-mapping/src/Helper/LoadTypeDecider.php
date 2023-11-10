<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Exception\InvalidInputException;

class LoadTypeDecider
{
    public static function canClone(array $tableInfo, string $workspaceType, array $exportOptions): bool
    {
        if ($tableInfo['isAlias'] && (empty($tableInfo['aliasColumnsAutoSync']) || !empty($tableInfo['aliasFilter']))) {
            return false;
        }

        if (array_keys($exportOptions) !== ['overwrite'] ||
            ($tableInfo['bucket']['backend'] !== $workspaceType) ||
            ($workspaceType !== 'snowflake')
        ) {
            return false;
        }
        return true;
    }

    public static function canUseView(
        array $tableInfo,
        string $workspaceType,
        array $exportOptions,
        string $currentProjectId,
    ): bool {
        if (($tableInfo['bucket']['backend'] !== $workspaceType) ||
            ($workspaceType !== 'bigquery')
        ) {
            return false;
        }

        if ($exportOptions && array_keys($exportOptions) !== ['overwrite']) {
            throw new InvalidInputException(sprintf(
                'Option "%s" is not supported when loading Bigquery table "%s".',
                implode(', ', array_keys($exportOptions)),
                $tableInfo['id'],
            ));
        }

        /* isAlias means that the table is EITHER an alias OR a table shared from a different project.
            Surprisingly, the table shared from different project IS supported, but the alias is not.
            https://keboolaglobal.slack.com/archives/C055HSMKX51/p1699434828910109
        */
        if ($tableInfo['isAlias'] && ((string) $tableInfo['sourceTable']['project']['id'] === $currentProjectId)) {
            throw new InvalidInputException(sprintf(
                'Table "%s" is an alias, which is not supported when loading Bigquery tables.',
                $tableInfo['id'],
            ));
        }

        return true;
    }
}
