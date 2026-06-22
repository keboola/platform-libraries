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
 * Stores table/column descriptions for tables that are NOT freshly created through a table definition
 * (i.e. pre-existing tables and non-typed tables created from a CSV header).
 *
 * For these the description is written as `KBC.description` metadata under the component provider. The
 * Storage read precedence (native first-class description set by a user in the UI takes priority over the
 * component-provided key/value metadata) ensures this never overwrites a description set manually by a user.
 *
 * Freshly created typed/schema tables carry the description directly in the table-definition request (native
 * field) and are skipped here - see LoadTableTaskCreator.
 */
class DescriptionSetter
{
    private const DESCRIPTION_METADATA_KEY = 'KBC.description';

    public function setDescription(
        LoadTableTaskInterface $loadTask,
        MappingFromProcessedConfiguration $processedSource,
        SystemMetadata $systemMetadata,
    ): LoadTableTaskInterface {
        if ($loadTask->isDescriptionInTableDefinition()) {
            return $loadTask;
        }

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
