<?php

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;

class Scope
{
    const TABLE_DATA = 'tableData';
    const TABLE_METADATA = 'tableMetadata';
    const FILE_DATA = 'fileData';
    const FILE_METADATA = 'fileMetadata';

    /** @var array */
    private $scopeTypes;

    public function __construct(array $scopeTypes)
    {
        $allowedScopeTypes = [
            self::TABLE_DATA,
            self::TABLE_METADATA,
            self::FILE_DATA,
            self::FILE_METADATA
        ];
        if ($diff = array_diff($scopeTypes, $allowedScopeTypes)) {
            throw new StagingException(sprintf('Unknown scope types "%s".', implode(', ', $diff)));
        }
        $this->scopeTypes = $scopeTypes;
    }

    /**
     * @return string[]
     */
    public function getScopeTypes()
    {
        return $this->scopeTypes;
    }
}
