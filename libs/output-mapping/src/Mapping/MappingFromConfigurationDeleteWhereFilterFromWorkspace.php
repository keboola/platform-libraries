<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingFromConfigurationDeleteWhereFilterFromWorkspace extends AbstractMappingFromConfigurationDeleteWhereFilter
{
    public function getWorkspaceId(): string
    {
        return $this->mapping['values_from_workspace']['workspace_id'];
    }

    public function getWorkspaceTable(): string
    {
        return $this->mapping['values_from_workspace']['table'];
    }

    public function getWorkspaceColumn(): string
    {
        return $this->mapping['values_from_workspace']['column'];
    }
}
