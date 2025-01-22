<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingFromConfigurationDeleteWhereFilterFromWorkspace extends AbstractMappingFromConfigurationDeleteWhereFilter
{
    public function getWorkspaceId(): string
    {
        return $this->mapping['values_from_workspace']['workspaceId'];
    }

    public function getTableId(): string
    {
        return $this->mapping['values_from_workspace']['table'];
    }

    public function getColumn(): string
    {
        return $this->mapping['values_from_workspace']['column'];
    }
}
