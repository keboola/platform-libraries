<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Mapping\MappingTableLayout;

interface TableDefinitionWithLayoutInterface
{
    /** Partitioning and clustering to be rendered into the payload returned by getRequestData(). */
    public function setLayout(MappingTableLayout $layout): void;
}
