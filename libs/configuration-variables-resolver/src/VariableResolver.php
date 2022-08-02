<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\StorageApiBranch\ClientWrapper;
use Mustache_Engine;
use Psr\Log\LoggerInterface;

class VariableResolver
{
    private ClientWrapper $clientWrapper;

    private Mustache_Engine $moustache;

    private LoggerInterface $logger;

    private ComponentsClientHelper $componentsHelper;

    public function __construct(ClientWrapper $clientWrapper, LoggerInterface $logger)
    {
        $this->clientWrapper = $clientWrapper;
        $this->moustache = new Mustache_Engine([
            'escape' => function ($string) {
                return trim((string) json_encode($string), '"');
            },
        ]);
        $this->logger = $logger;
        $this->componentsHelper = new ComponentsClientHelper($this->clientWrapper);
    }

    public function resolveVariables(array $configuration, ?string $variableValuesId, ?array $variableValuesData): array
    {
        if ($variableValuesId && !empty($variableValuesData['values'])) {
            throw new UserException('Only one of variableValuesId and variableValuesData can be entered.');
        }

        $variablesId = $configuration['variables_id'] ?? null;
        $defaultValuesId = $configuration['variables_values_id'] ?? null;

        if ($variablesId) {
            $vConfiguration = $this->componentsHelper->getVariablesConfiguration($variablesId);

            if (!empty($variableValuesData['values'])) {
                $this->logger->info('Replacing variables using inline values.');
                $vRow = $variableValuesData;
            } elseif ($variableValuesId) {
                $this->logger->info(sprintf('Replacing variables using values with ID: "%s".', $variableValuesId));
                $vRow = $this->componentsHelper->getVariablesConfigurationRow($variablesId, $variableValuesId);
            } elseif ($defaultValuesId) {
                $this->logger->info(
                    sprintf('Replacing variables using default values with ID: "%s"', $defaultValuesId)
                );
                $vRow = $this->componentsHelper->getVariablesConfigurationRow($variablesId, $defaultValuesId);
            } else {
                throw new UserException(sprintf(
                    'No variable values provided for variables configuration "%s".',
                    $variablesId
                ));
            }

            $context = $this->createContext($vConfiguration, $vRow);

            return $this->renderConfiguration($configuration, $context);
        }

        return $configuration;
    }

    private function createContext(array $variablesConfiguration, array $variablesRow): VariablesContext
    {
        $context = new VariablesContext($variablesRow);
        $variableNames = [];
        foreach ($variablesConfiguration['variables'] as $variable) {
            $variableNames[] = $variable['name'];
            if (!$context->__isset((string) $variable['name'])) {
                throw new UserException(sprintf('No value provided for variable "%s".', $variable['name']));
            }
        }
        $this->logger->info(sprintf('Replaced values for variables: "%s".', implode(', ', $variableNames)));

        return $context;
    }

    private function renderConfiguration(array $configuration, VariablesContext $context): array
    {
        $newConfiguration = json_decode(
            $this->moustache->render((string) json_encode($configuration), $context),
            true
        );
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new UserException(
                'Variable replacement resulted in invalid configuration, error: ' . json_last_error_msg()
            );
        }
        if ($context->getMissingVariables()) {
            throw new UserException(sprintf(
                'Missing values for placeholders: "%s"',
                implode(', ', $context->getMissingVariables())
            ));
        }

        return (array) $newConfiguration;
    }
}
