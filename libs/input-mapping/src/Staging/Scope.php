<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;

class Scope
{
    public const TABLE_DATA = 'tableData';
    public const TABLE_METADATA = 'tableMetadata';
    public const FILE_DATA = 'fileData';
    public const FILE_METADATA = 'fileMetadata';

    private array $scopeTypes;

    public function __construct(array $scopeTypes)
    {
        $allowedScopeTypes = [
            self::TABLE_DATA,
            self::TABLE_METADATA,
            self::FILE_DATA,
            self::FILE_METADATA,
        ];
        $diff = array_diff($scopeTypes, $allowedScopeTypes);
        if ($diff) {
            throw new StagingException(sprintf('Unknown scope types "%s".', implode(', ', $diff)));
        }
        $this->scopeTypes = $scopeTypes;
    }

    /**
     * @return string[]
     */
    public function getScopeTypes(): array
    {
        return $this->scopeTypes;
    }
}
