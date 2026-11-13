<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\TableDefinition;

use Keboola\OutputMapping\Mapping\MappingTableLayout;

trait TableDefinitionLayoutTrait
{
    private ?MappingTableLayout $layout = null;

    public function setLayout(MappingTableLayout $layout): void
    {
        $this->layout = $layout;
    }

    /**
     * @param array<mixed> $requestData
     * @return array<mixed>
     */
    private function withLayout(array $requestData): array
    {
        if ($this->layout === null || $this->layout->isEmpty()) {
            return $requestData;
        }

        return array_merge($requestData, $this->layout->toRequestData());
    }
}
