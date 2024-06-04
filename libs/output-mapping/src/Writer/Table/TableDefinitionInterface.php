<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

interface TableDefinitionInterface
{
    public function getRequestData(): array;
}
