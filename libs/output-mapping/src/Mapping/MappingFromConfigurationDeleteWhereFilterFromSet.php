<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingFromConfigurationDeleteWhereFilterFromSet extends AbstractMappingFromConfigurationDeleteWhereFilter
{
    public function getValues(): array
    {
        return $this->mapping['values_from_set'];
    }
}
