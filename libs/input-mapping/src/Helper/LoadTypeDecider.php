<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Exception\InvalidInputException;

class LoadTypeDecider
{
    public static function checkViableLoadMethod(
        array $tableInfo,
        string $workspaceType,
        array $exportOptions,
        string $currentProjectId,
    ): void {
        $isWorkspaceBigQuery = $workspaceType === 'bigquery';
        $isBackendMismatch = $tableInfo['bucket']['backend'] !== $workspaceType;
        $hasOtherThanOverwriteOptions = $exportOptions && array_keys($exportOptions) !== ['overwrite'];
        $isAliasInCurrentProject = $tableInfo['isAlias'] &&
            ((string) $tableInfo['sourceTable']['project']['id'] === $currentProjectId);

        if ($isWorkspaceBigQuery) {
            if ($isBackendMismatch) {
                throw new InvalidInputException(sprintf(
                    'Workspace type "%s" does not match table backend type "%s" when loading Bigquery table "%s".',
                    $workspaceType,
                    $tableInfo['bucket']['backend'],
                    $tableInfo['id'],
                ));
            }

            if ($hasOtherThanOverwriteOptions) {
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
            if ($isAliasInCurrentProject) {
                throw new InvalidInputException(sprintf(
                    'Table "%s" is an alias, which is not supported when loading Bigquery tables.',
                    $tableInfo['id'],
                ));
            }
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
    ): bool {
        $backend = $tableInfo['bucket']['backend'];
        $isBackendMatch = $backend === $workspaceType;
        if ($isBackendMatch && $workspaceType === 'bigquery') {
            return true;
        }

        if ($isBackendMatch
            && $backend === 'snowflake'
            && array_key_exists('hasExternalSchema', $tableInfo['bucket'])
            && $tableInfo['bucket']['hasExternalSchema'] === true
        ) {
            // allow view for buckets with external schema
            return true;
        }

        return false;
    }
}
