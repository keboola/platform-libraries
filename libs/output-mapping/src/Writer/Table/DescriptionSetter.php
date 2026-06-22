<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnsMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\TableMetadata;
use Keboola\OutputMapping\Mapping\MappingColumnMetadata;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\SystemMetadata;

/**
 * Records table/column descriptions as `KBC.description` metadata under the component provider, so the
 * description is attributed to the component - consistently with the other metadata written by output mapping
 * (table_metadata, column_metadata) and regardless of whether the table is freshly created or already exists.
 *
 * Storage read precedence (the native first-class description, e.g. one set by a user in the UI, takes priority
 * over the component-provided key/value metadata) ensures this never overwrites a description set manually by a
 * user. Freshly created tables additionally carry the description directly in the table-definition request so it
 * lands on the native field at creation time - see LoadTableTaskCreator.
 */
class DescriptionSetter
{
    private const DESCRIPTION_METADATA_KEY = 'KBC.description';

    public function setDescription(
        LoadTableTaskInterface $loadTask,
        MappingFromProcessedConfiguration $processedSource,
        SystemMetadata $systemMetadata,
    ): LoadTableTaskInterface {
        $tableId = $processedSource->getDestination()->getTableId();
        $provider = (string) $systemMetadata->getSystemMetadata(SystemMetadata::SYSTEM_KEY_COMPONENT_ID);

        $tableDescription = $processedSource->getTableDescription();
        if ($tableDescription !== null) {
            $loadTask->addMetadata(new TableMetadata(
                $tableId,
                $provider,
                [
                    [
                        'key' => self::DESCRIPTION_METADATA_KEY,
                        'value' => $tableDescription,
                    ],
                ],
            ));
        }

        $columnDescriptions = $processedSource->getColumnDescriptions();
        if ($columnDescriptions) {
            $columnMetadata = [];
            foreach ($columnDescriptions as $columnName => $description) {
                $columnMetadata[] = new MappingColumnMetadata(
                    (string) $columnName,
                    [
                        [
                            'key' => self::DESCRIPTION_METADATA_KEY,
                            'value' => $description,
                        ],
                    ],
                );
            }
            $loadTask->addMetadata(new ColumnsMetadata($tableId, $provider, $columnMetadata));
        }

        return $loadTask;
    }
}
