<?php

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;

class Operation
{
    const TABLE_DATA = 'tableData';
    const TABLE_METADATA = 'tableMetadata';
    const FILE_DATA = 'fileData';
    const FILE_METADATA = 'fileMetadata';

    /** @var array */
    private $operationTypes;

    public function __construct(array $operationTypes)
    {
        $allowedOperationTypes = [
            self::TABLE_DATA,
            self::TABLE_METADATA,
            self::FILE_DATA,
            self::FILE_METADATA
        ];
        if ($diff = array_diff($operationTypes, $allowedOperationTypes)) {
            throw new StagingException(sprintf('Unknown operation types "%s".', implode(', ', $diff)));
        }
        $this->operationTypes = $operationTypes;
    }

    /**
     * @return string[]
     */
    public function getOperationTypes()
    {
        return $this->operationTypes;
    }
}
