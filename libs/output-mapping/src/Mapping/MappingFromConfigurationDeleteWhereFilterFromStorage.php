<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

class MappingFromConfigurationDeleteWhereFilterFromStorage extends AbstractMappingFromConfigurationDeleteWhereFilter
{
    public function getStorageBucketId(): string
    {
        return $this->mapping['values_from_storage']['bucket_id'];
    }

    public function getStorageTable(): string
    {
        return $this->mapping['values_from_storage']['table'];
    }

    public function getStorageColumn(): ?string
    {
        return $this->mapping['values_from_storage']['column'] ?? null;
    }
}
