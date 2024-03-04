<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Storage\TableInfo;

class MappingStorageSources
{
    public function __construct(readonly private BucketInfo $bucket, readonly private ?TableInfo $table)
    {
    }

    public function getBucket(): BucketInfo
    {
        return $this->bucket;
    }

    public function hasTable(): bool
    {
        return $this->table !== null;
    }

    public function getTable(): ?TableInfo
    {
        return $this->table;
    }
}