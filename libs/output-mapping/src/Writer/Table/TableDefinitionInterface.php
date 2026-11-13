<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Mapping\MappingTableLayout;
use Keboola\OutputMapping\Storage\TableDescription;
use Psr\Log\LoggerInterface;

interface TableDefinitionInterface
{
    public function getRequestData(): array;

    public function getTableName(): string;

    /**
     * Descriptions to be rendered into the payload returned by getRequestData(), so that a table created from
     * this definition carries them right away.
     */
    public function setDescriptions(TableDescription $descriptions, LoggerInterface $logger): void;

    /** Partitioning and clustering to be rendered into the payload returned by getRequestData(). */
    public function setLayout(MappingTableLayout $layout): void;
}
