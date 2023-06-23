<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

use JsonException;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Mustache_Engine;
use Psr\Log\LoggerInterface;

class VariablesRenderer
{
    private readonly Mustache_Engine $moustache;

    public function __construct(
        private readonly LoggerInterface $logger,
    ) {
        $this->moustache = new Mustache_Engine([
            'escape' => fn($string) => trim((string) json_encode($string), '"'),
        ]);
    }

    /**
     * @param array<non-empty-string, string> $variables
     */
    public function renderVariables(array $configuration, array $variables): array
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
            'Replaced values for variables: %s',
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
