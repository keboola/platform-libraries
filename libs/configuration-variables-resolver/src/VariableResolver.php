<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver;

use JsonException;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesLoader\ConfigurationVariablesLoader;
use Keboola\ConfigurationVariablesResolver\VariablesLoader\VaultVariablesLoader;
use Keboola\StorageApiBranch\ClientWrapper;
use Mustache_Engine;
use Psr\Log\LoggerInterface;

class VariableResolver
{
    private readonly Mustache_Engine $moustache;

    public function __construct(
        private readonly ConfigurationVariablesLoader $configurationVariablesLoader,
        private readonly VaultVariablesLoader $vaultVariablesLoader,
        private readonly LoggerInterface $logger,
    ) {
        $this->moustache = new Mustache_Engine([
            'escape' => fn($string) => trim((string) json_encode($string), '"'),
        ]);
    }

    public static function create(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
    ): self {
        $componentsClientHelper = new ComponentsClientHelper($clientWrapper);

        return new self(
            new ConfigurationVariablesLoader($componentsClientHelper, $logger),
            new VaultVariablesLoader(),
            $logger,
        );
    }

    /**
     * @param array{variables_id?: string|null, variables_values_id?: string|null} $configuration
     * @param non-empty-string|null $variableValuesId
     * @param array{values?: list<array{name: scalar, value: scalar}>|null}|null $variableValuesData
     */
    public function resolveVariables(array $configuration, ?string $variableValuesId, ?array $variableValuesData): array
    {
        $variables = $this->loadVariables($configuration, $variableValuesId, $variableValuesData);
        return $this->renderConfiguration($configuration, $variables);
    }

    /**
     * @param array{variables_id?: string|null, variables_values_id?: string|null} $configuration
     * @param non-empty-string|null $variableValuesId
     * @param array{values?: list<array{name: scalar, value: scalar}>|null}|null $variableValuesData
     * @return array<non-empty-string, string>
     */
    private function loadVariables(array $configuration, ?string $variableValuesId, ?array $variableValuesData): array
    {
        // !!! do not use array_merge() here as it can break numeric keys, array union is safe
        return
            $this->configurationVariablesLoader->loadVariables($configuration, $variableValuesId, $variableValuesData) +
            $this->vaultVariablesLoader->loadVariables($configuration)
        ;
    }

    /**
     * @param array<non-empty-string, string> $variables
     */
    private function renderConfiguration(array $configuration, array $variables): array
    {
        $context = new VariablesContext($variables);

        $renderedConfiguration = $this->moustache->render(
            (string) json_encode($configuration, JSON_THROW_ON_ERROR),
            $context,
        );

        if ($context->getMissingVariables()) {
            throw new UserException(sprintf(
                'Missing values for placeholders: %s',
                implode(', ', $context->getMissingVariables())
            ));
        }

        $this->logger->info(sprintf(
            'Replaced values for variables: %s.',
            implode(', ', $context->getReplacedVariables()),
        ));

        try {
            return (array) json_decode($renderedConfiguration, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new UserException(
                'Variable replacement resulted in invalid configuration, error: ' . $e->getMessage()
            );
        }
    }
}
