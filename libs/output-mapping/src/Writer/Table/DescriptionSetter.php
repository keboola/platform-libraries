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
        // Description is written to the first-class Storage description field, which has no per-provider
        // precedence: the last writer wins. To avoid overwriting a description a user set manually in the
        // UI, we only push the configured description for tables freshly created by this job. Updating
        // descriptions on pre-existing tables is intentionally left to a future, user-edit-aware mechanism
        // (see AJDA-2714 and the "Description propagation: Storage" milestone open questions).
        if (!$loadTask->isUsingFreshlyCreatedTable()) {
            return $loadTask;
        }

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
