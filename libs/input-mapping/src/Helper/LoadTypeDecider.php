<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Exception\InvalidInputException;

class LoadTypeDecider
{
    public static function checkViableBigQueryLoadMethod(
        array $tableInfo,
        string $workspaceType,
    ): void {
        if ($tableInfo['bucket']['backend'] !== 'bigquery') {
            throw new InvalidInputException(sprintf(
                'Workspace type "%s" does not match table backend type "%s" when loading Bigquery table "%s".',
                $workspaceType,
                $tableInfo['bucket']['backend'],
                $tableInfo['id'],
            ));
        }
    }

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

        if (array_key_exists('hasExternalSchema', $tableInfo['bucket'])
            && $tableInfo['bucket']['hasExternalSchema'] === true
        ) {
            // clone is not allowed for buckets with external schema
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
        $backend = $tableInfo['bucket']['backend'];
        $isBackendMatch = $backend === $workspaceType;

        // backend mismatch, view is not supported
        if (!$isBackendMatch) {
            return false;
        }

        // BigQuery always supports views when backend matches
        // Snowflake with external schema also supports views
        // For other cases, additional validation is required
        if ($workspaceType !== 'bigquery'
            && !($backend === 'snowflake'
                && array_key_exists('hasExternalSchema', $tableInfo['bucket'])
                && $tableInfo['bucket']['hasExternalSchema'] === true)
        ) {
            return false;
        }

        $hasOtherThanOverwriteOptions = $exportOptions && array_keys($exportOptions) !== ['overwrite'];
        if ($hasOtherThanOverwriteOptions) {
            return false; // aka filters are not allowed
        }

        /* isAlias means that the table is EITHER an alias OR a table shared from a different project.
            Surprisingly, the table shared from different project IS supported, but the alias is not.
            https://keboolaglobal.slack.com/archives/C055HSMKX51/p1699434828910109
        */
        $isAliasInCurrentProject = $tableInfo['isAlias'] &&
            ((string) $tableInfo['sourceTable']['project']['id'] === $currentProjectId);
        if ($isAliasInCurrentProject) {
            return false;
        }

        return true;
    }
}
