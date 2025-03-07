<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

readonly class UnifiedConfigurationResolver
{
    /**
     * @param non-empty-string $branchId
     * @param non-empty-string|null $variableValuesId
     */
    public function __construct(
        private SharedCodeResolver $sharedCodeResolver,
        private VariablesResolver $variablesResolver,
        private string $branchId,
        private ?string $variableValuesId = null,
        private ?array $variableValuesData = null,
    ) {
    }

    /**
     * Resolves configuration by applying both shared code and variables resolution.
     */
    public function resolveConfiguration(
        array $configuration,
    ): ResolveResults {
        return $this->variablesResolver->resolveVariables(
            configuration: $this->sharedCodeResolver->resolveSharedCode($configuration),
            branchId: $this->branchId,
            variableValuesId: $this->variableValuesId,
            variableValuesData: $this->variableValuesData,
        );
    }
}
