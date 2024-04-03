<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use Keboola\OutputMapping\Writer\Table\MappingResolver\MappingResolverInterface;
use Keboola\OutputMapping\Writer\Table\MappingResolver\WorkspaceMappingResolver;
use Keboola\OutputMapping\Writer\Table\Source\SqlWorkspaceItemSourceFactory;

class SqlWorkspaceTableStrategy extends AbstractWorkspaceTableStrategy
{
    public function getMappingResolver(): MappingResolverInterface
    {
        return new WorkspaceMappingResolver(
            $this->metadataStorage->getPath(),
            new SqlWorkspaceItemSourceFactory($this->dataStorage),
        );
    }
}
