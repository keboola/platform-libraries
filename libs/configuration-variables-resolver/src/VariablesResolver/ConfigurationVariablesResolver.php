<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesResolver;

use Keboola\ConfigurationVariablesResolver\ComponentsClientHelper;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\MustacheRenderer;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RenderResults;
use Psr\Log\LoggerInterface;

class ConfigurationVariablesResolver
{
    public function __construct(
        private readonly ComponentsClientHelper $componentsHelper,
        private readonly MustacheRenderer $renderer,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @param non-empty-string|null $variableValuesId
     * @param array{values?: list<array{name: scalar, value: scalar}>|null}|null $variableValuesData
     */
    public function resolveVariables(
        array $configuration,
        ?string $variableValuesId,
        ?array $variableValuesData,
    ): RenderResults {
        if ($variableValuesId && !empty($variableValuesData['values'])) {
            throw new UserException('Only one of variableValuesId and variableValuesData can be entered.');
        }

        $variablesId = $configuration['variables_id'] ?? null;
        $variablesValuesId = $configuration['variables_values_id'] ?? null;

        if (!$variablesId) {
            return new RenderResults(
                $configuration,
                [],
                [],
                [],
            );
        }

        $configurationVariables = $this->loadVariables(
            $variablesId,
            $variablesValuesId,
            $variableValuesId,
            $variableValuesData,
        );

        return $this->renderer->renderVariables(
            $configuration,
            $configurationVariables,
        );
    }


    /**
     * @param non-empty-string $variablesId
     * @param non-empty-string|null $variablesValuesId
     * @param non-empty-string|null $variableValuesId
     * @param array{values?: list<array{name: scalar, value: scalar}>|null}|null $variableValuesData
     * @return array<non-empty-string, string>
     */
    public function loadVariables(
        string $variablesId,
        ?string $variablesValuesId,
        ?string $variableValuesId,
        ?array $variableValuesData,
    ): array {
        $variablesConfiguration = $this->componentsHelper->getVariablesConfiguration($variablesId);
        $variablesData = $this->loadVariablesData(
            $variablesId,
            $variablesValuesId,
            $variableValuesId,
            $variableValuesData,
        );

        $keyValue = [];
        foreach ($variablesData['values'] as $row) {
            $key = (string) $row['name'];
            $value = (string) $row['value'];

            if ($key === '') {
                throw new UserException('Variable name cannot be empty.');
            }

            $keyValue[$key] = $value;
        }

        foreach ($variablesConfiguration['variables'] as $variable) {
            if (!array_key_exists($variable['name'], $keyValue)) {
                throw new UserException(sprintf('No value provided for variable "%s".', $variable['name']));
            }
        }

        return $keyValue;
    }

    /**
     * @param non-empty-string $variablesId
     * @param array{values?: list<array{name: scalar, value: scalar}>|null}|null $variableValuesData
     * @return array{values: list<array{name: scalar, value: scalar}>}
     */
    private function loadVariablesData(
        string $variablesId,
        ?string $variableValuesId,
        ?string $rowVariableValuesId,
        ?array $variableValuesData,
    ): array {
        if (!empty($variableValuesData['values'])) {
            $this->logger->info('Replacing variables using inline values.');
            return $variableValuesData;
        }

        if ($rowVariableValuesId) {
            $this->logger->info(sprintf(
                'Replacing variables using values with ID: "%s".',
                $rowVariableValuesId,
            ));
            return $this->componentsHelper->getVariablesConfigurationRow(
                $variablesId,
                $rowVariableValuesId,
            );
        }

        if ($variableValuesId) {
            $this->logger->info(sprintf(
                'Replacing variables using default values with ID: "%s"',
                $variableValuesId,
            ));
            return $this->componentsHelper->getVariablesConfigurationRow(
                $variablesId,
                $variableValuesId,
            );
        }

        throw new UserException(sprintf(
            'No variable values provided for variables configuration "%s".',
            $variablesId,
        ));
    }
}
