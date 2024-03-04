<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Storage;

class BucketInfo
{
    public readonly string $id;
    public readonly string $backend;
    public function __construct(private readonly array $bucketInfo) {
        $this->id = (string) $this->bucketInfo['id'];
        $this->backend = $this->bucketInfo['backend'];
    }
}
