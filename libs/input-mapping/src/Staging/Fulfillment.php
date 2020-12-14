<?php

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;

class Fulfillment
{
    const TABLE_DATA = 'tableData';
    const TABLE_METADATA = 'tableMetadata';
    const FILE_DATA = 'fileData';
    const FILE_METADATA = 'fileMetadata';

    /** @var array */
    private $fulfillmentTypes;

    public function __construct(array $fulfillmentTypes)
    {
        $allowedFulfillmentTypes = [
            self::TABLE_DATA,
            self::TABLE_METADATA,
            self::FILE_DATA,
            self::FILE_METADATA
        ];
        if ($diff = array_diff($fulfillmentTypes, $allowedFulfillmentTypes)) {
            throw new StagingException(sprintf('Unknown fulfillment types "%s".', implode(', ', $diff)));
        }
        $this->fulfillmentTypes = $fulfillmentTypes;
    }

    /**
     * @return string[]
     */
    public function getFulfillmentTypes()
    {
        return $this->fulfillmentTypes;
    }
}
