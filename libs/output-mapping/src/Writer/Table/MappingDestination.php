<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use InvalidArgumentException;

class MappingDestination
{
    private string $bucketStage;
    private string $bucketName;
    private string $tableName;

    public function __construct(string $value)
    {
        $parts = explode('.', $value);
        if (count($parts) !== 3) {
            throw new InvalidArgumentException('Value is not a valid table ID');
        }

        $this->bucketStage = $parts[0];
        $this->bucketName = $parts[1];
        $this->tableName = $parts[2];
    }

    public static function isTableId(?string $value): bool
    {
        return is_string($value) && substr_count($value, '.') === 2;
    }

    public function getBucketId(): string
    {
        return $this->bucketStage . '.' . $this->bucketName;
    }

    public function getTableId(): string
    {
        return $this->getBucketId() . '.' . $this->tableName;
    }

    public function getBucketStage(): string
    {
        return $this->bucketStage;
    }

    public function getBucketName(): string
    {
        if (str_starts_with($this->bucketName, 'c-')) {
            return substr($this->bucketName, 2);
        }
        return $this->bucketName;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }
}
