<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

public interface TableDefinitionColumnInterface
{
    public function getName(): string;

    public function toArray(): array;
}
