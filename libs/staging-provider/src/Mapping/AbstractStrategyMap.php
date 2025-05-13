<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Mapping;

use Keboola\StagingProvider\Exception\InvalidStagingConfiguration;
use Keboola\StagingProvider\Staging\StagingType;

/**
 * @template-covariant T_FILE_STRATEGY of object
 * @template-covariant T_TABLE_STRATEGY of object
 */
abstract class AbstractStrategyMap
{
    private ?array $strategyMap = null;

    /**
     * @return iterable<StagingDefinition<T_FILE_STRATEGY, T_TABLE_STRATEGY>>
     */
    abstract protected function provideStagingDefinitions(): iterable;

    /**
     * @return array<non-empty-string, StagingDefinition<T_FILE_STRATEGY, T_TABLE_STRATEGY>>
     */
    public function getStrategyMap(): array
    {
        if ($this->strategyMap === null) {
            $this->strategyMap = [];
            foreach ($this->provideStagingDefinitions() as $definition) {
                $this->strategyMap[$definition->type->value] = $definition;
            }
        }

        return $this->strategyMap;
    }

    /**
     * @param StagingType $stagingType
     * @return StagingDefinition<T_FILE_STRATEGY, T_TABLE_STRATEGY>
     */
    public function getStagingDefinition(StagingType $stagingType): StagingDefinition
    {
        return $this->getStrategyMap()[$stagingType->value] ?? throw new InvalidStagingConfiguration(
            sprintf(
                'Mapping on type "%s" is not supported. Supported types are "%s".',
                $stagingType->value,
                implode(', ', array_keys($this->getStrategyMap())),
            ),
        );
    }
}
