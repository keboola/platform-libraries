<?php

namespace Keboola\OutputMapping\Writer\Table;

use InvalidArgumentException;

class MappingDestination
{
    /** @var string */
    private $bucketStage;

    /** @var string */
    private $bucketName;

    /** @var string */
    private $tableName;

    /**
     * @param string $value
     */
    public function __construct($value)
    {
        if (!is_string($value)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $value must be a string, %s given',
                is_object($value) ? get_class($value) : gettype($value)
            ));
        }

        $parts = explode('.', $value);
        if (count($parts) !== 3) {
            throw new InvalidArgumentException('Value is not a valid table ID');
        }

        $this->bucketStage = $parts[0];
        $this->bucketName = $parts[1];
        $this->tableName = $parts[2];
    }

    /**
     * @param string $value
     * @return bool
     */
    public static function isTableId($value)
    {
        return is_string($value) && substr_count($value, '.') === 2;
    }

    /**
     * @return string
     */
    public function getBucketId()
    {
        return $this->bucketStage . '.' . $this->bucketName;
    }

    /**
     * @return string
     */
    public function getTableId()
    {
        return $this->getBucketId() . '.' . $this->tableName;
    }

    /**
     * @return string
     */
    public function getBucketStage()
    {
        return $this->bucketStage;
    }

    /**
     * @return string
     */
    public function getBucketName()
    {
        if (substr($this->bucketName, 0, 2) === 'c-') {
            return substr($this->bucketName, 2);
        }
        return $this->bucketName;
    }

    /**
     * @return string
     */
    public function getTableName()
    {
        return $this->tableName;
    }
}
