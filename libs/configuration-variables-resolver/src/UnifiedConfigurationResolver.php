<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

class UnifiedConfigurationResolver
{
    /**
     * @param non-empty-string $branchId
     * @param non-empty-string|null $variableValuesId
     */
    public function __construct(
        private readonly SharedCodeResolver $sharedCodeResolver,
        private readonly VariablesResolver $variablesResolver,
        private readonly string $branchId,
        private readonly ?string $variableValuesId = null,
        private readonly ?array $variableValuesData = null,
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
