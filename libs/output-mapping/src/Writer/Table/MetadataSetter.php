<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\SchemaColumnMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\TableMetadata;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Writer\AbstractWriter;
use Keboola\OutputMapping\Writer\TableWriter;

class MetadataSetter
{
    public function setTableMetadata(
        LoadTableTaskInterface $loadTask,
        MappingFromProcessedConfiguration $processedSource,
        MappingStorageSources $storageSources,
        SystemMetadata $systemMetadata,
    ): LoadTableTaskInterface {
        if (!$storageSources->didTableExistBefore()) {
            $loadTask->addMetadata(new TableMetadata(
                $processedSource->getDestination()->getTableId(),
                TableWriter::SYSTEM_METADATA_PROVIDER,
                $systemMetadata->getCreatedMetadata(),
            ));
        }

        $loadTask->addMetadata(new TableMetadata(
            $processedSource->getDestination()->getTableId(),
            TableWriter::SYSTEM_METADATA_PROVIDER,
            $systemMetadata->getUpdatedMetadata(),
        ));

        if ($processedSource->hasMetadata()) {
            $loadTask->addMetadata(new TableMetadata(
                $processedSource->getDestination()->getTableId(),
                (string) $systemMetadata->getSystemMetadata(AbstractWriter::SYSTEM_KEY_COMPONENT_ID),
                $processedSource->getMetadata(),
            ));
        }
        if ($processedSource->hasTableMetadata()) {
            $loadTask->addMetadata(new TableMetadata(
                $processedSource->getDestination()->getTableId(),
                (string) $systemMetadata->getSystemMetadata(AbstractWriter::SYSTEM_KEY_COMPONENT_ID),
                array_map(
                    fn(string $k, string $v) => ['key' => $k, 'value' => $v],
                    array_keys($processedSource->getTableMetadata()),
                    array_values($processedSource->getTableMetadata()),
                ),
            ));
        }

        if ($processedSource->hasColumnMetadata()) {
            $loadTask->addMetadata(new ColumnMetadata(
                $processedSource->getDestination()->getTableId(),
                (string) $systemMetadata->getSystemMetadata(AbstractWriter::SYSTEM_KEY_COMPONENT_ID),
                $processedSource->getColumnMetadata(),
            ));
        }

        if ($processedSource->getSchema() && $processedSource->hasSchemaColumnMetadata()) {
            $loadTask->addMetadata(new SchemaColumnMetadata(
                $processedSource->getDestination()->getTableId(),
                (string) $systemMetadata->getSystemMetadata(AbstractWriter::SYSTEM_KEY_COMPONENT_ID),
                $processedSource->getSchema(),
            ));
        }

        return $loadTask;
    }
}
