<?php

declare(strict_types=1);

namespace Keboola\OutputMapping;

use Keboola\OutputMapping\DeferredTasks\LoadTableTaskInterface;
use Keboola\OutputMapping\Storage\TableDescription;

/**
 * Outcome of LoadTableTaskCreator::create() - the load task itself plus the descriptions which the creator
 * could not hand over to Storage yet.
 */
class LoadTableTaskResult
{
    /**
     * @param TableDescription|null $descriptionsNotEmbeddedInCreatePayload descriptions which could not be
     *     part of a create-table-definition payload, because the table is created by the load job itself;
     *     null when there is nothing left to do - the descriptions were embedded in the create payload, they
     *     were already applied to a table which existed before, or there is no description at all
     */
    public function __construct(
        private readonly LoadTableTaskInterface $loadTableTask,
        private readonly ?TableDescription $descriptionsNotEmbeddedInCreatePayload = null,
    ) {
    }

    public function getLoadTableTask(): LoadTableTaskInterface
    {
        return $this->loadTableTask;
    }

    public function getDescriptionsNotEmbeddedInCreatePayload(): ?TableDescription
    {
        return $this->descriptionsNotEmbeddedInCreatePayload;
    }
}
