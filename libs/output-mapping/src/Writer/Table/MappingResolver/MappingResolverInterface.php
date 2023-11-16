<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\MappingResolver;

use Keboola\OutputMapping\Writer\Table\MappingSource;

interface MappingResolverInterface
{
    /**
     * @return MappingSource[]
     */
    public function resolveMappingSources(
        string $sourcePathPrefix,
        array $configuration,
        bool $isFailedJob,
        bool $useSliceFeature,
    ): array;
}
