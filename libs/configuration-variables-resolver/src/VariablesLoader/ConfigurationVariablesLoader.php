<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesLoader;

use Keboola\ConfigurationVariablesResolver\ComponentsClientHelper;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Psr\Log\LoggerInterface;

class ConfigurationVariablesLoader
{
    public function __construct(
        private readonly ComponentsClientHelper $componentsHelper,
        private readonly LoggerInterface $logger,
    ) {
    }

    /**
     * @param array{variables_id?: string|null, variables_values_id?: string|null} $configuration
     * @param non-empty-string|null $variableValuesId
     * @param array{values?: list<array{name: scalar, value: scalar}>|null}|null $variableValuesData
     * @return array<non-empty-string, string>
     */
    public function loadVariables(array $configuration, ?string $variableValuesId, ?array $variableValuesData): array
    {
        if ($variableValuesId && !empty($variableValuesData['values'])) {
            throw new UserException('Only one of variableValuesId and variableValuesData can be entered.');
        }

        $variablesId = $configuration['variables_id'] ?? null;
        $variablesValuesId = $configuration['variables_values_id'] ?? null;

        if (!$variablesId) {
            return [];
        }

        $variablesConfiguration = $this->componentsHelper->getVariablesConfiguration($variablesId);
        $variablesData = $this->loadVariablesData(
            $variablesId,
            $variablesValuesId,
            $variableValuesId,
            $variableValuesData,
        );

        $values = [];
        foreach ($variablesData['values'] as $row) {
            $key = (string) $row['name'];
            $value = (string) $row['value'];

            if ($key === '') {
                throw new UserException('Variable name cannot be empty.');
            }

            $values[$key] = $value;
        }

        foreach ($variablesConfiguration['variables'] as $variable) {
            if (!array_key_exists($variable['name'], $values)) {
                throw new UserException(sprintf('No value provided for variable "%s".', $variable['name']));
            }
        }

        return $values;
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
            $variablesId
        ));
    }
}
