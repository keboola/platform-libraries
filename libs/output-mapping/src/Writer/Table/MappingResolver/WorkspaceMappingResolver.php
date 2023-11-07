<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\MappingResolver;

use Exception;

class WorkspaceMappingResolver extends AbstractMappingResolver
{
    public function resolveMappingSources(string $sourcePathPrefix, array $configuration, bool $isFailedJob): array
    {
        throw new Exception('Not imlemented yet');
    }
}
