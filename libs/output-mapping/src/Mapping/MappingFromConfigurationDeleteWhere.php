<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;

class MappingFromConfigurationDeleteWhere
{
    private const SUPPORTED_WHERE_FILTERS_CLASS_MAP = [
        'values_from_set' => MappingFromConfigurationDeleteWhereFilterFromSet::class,
        'values_from_workspace' => MappingFromConfigurationDeleteWhereFilterFromWorkspace::class,
        'values_from_storage' => MappingFromConfigurationDeleteWhereFilterFromStorage::class,
    ];

    public function __construct(
        private readonly array $mapping,
    ) {
    }

    public function getChangedSince(): ?string
    {
        return $this->mapping['changed_since'] ?? null;
    }

    public function getChangedUntil(): ?string
    {
        return $this->mapping['changed_until'] ?? null;
    }

    /**
     * @return (MappingFromConfigurationDeleteWhereFilterFromSet|MappingFromConfigurationDeleteWhereFilterFromWorkspace|MappingFromConfigurationDeleteWhereFilterFromStorage)[]|null
     */
    public function getWhereFilters(): ?array
    {
        if (!isset($this->mapping['where_filters'])) {
            return null;
        }

        return array_map(
            function (array $filter): AbstractMappingFromConfigurationDeleteWhereFilter {
                foreach (self::SUPPORTED_WHERE_FILTERS_CLASS_MAP as $key => $filterClass) {
                    if (isset($filter[$key])) {
                        return new $filterClass($filter);
                    }
                }

                throw new InvalidOutputException('Invalid filter type specified');
            },
            $this->mapping['where_filters'],
        );
    }
}
