<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\DeferredTasks\TableDefinition\TableDefinitionDescription;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;

class DescriptionSetter
{
    public function setDescription(
        LoadTableTaskInterface $loadTask,
        MappingFromProcessedConfiguration $processedSource,
    ): LoadTableTaskInterface {
        $description = new TableDefinitionDescription(
            $processedSource->getDestination()->getTableId(),
            $processedSource->getTableDescription(),
            $processedSource->getColumnDescriptions(),
        );

        if ($description->hasChanges()) {
            $loadTask->setDescription($description);
        }

        return $loadTask;
    }
}
