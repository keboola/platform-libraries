<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Mapping;

use Keboola\StagingProvider\Staging\StagingType;

/**
 * @template-covariant T_FILE_STRATEGY of object
 * @template-covariant T_TABLE_STRATEGY of object
 */
readonly class StagingDefinition
{
    public function __construct(
        public StagingType $type,
        /** @var class-string<T_FILE_STRATEGY> */
        public string $fileStrategyClass,
        /** @var class-string<T_TABLE_STRATEGY> */
        public string $tableStrategyClass,
    ) {
    }
}
