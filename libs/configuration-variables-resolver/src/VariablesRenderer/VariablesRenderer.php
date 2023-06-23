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
            // value is always string, so after escaping it using json_encode, it has extra " around it
            // originally we have used trim((string) json_encode($string), '"') to remove quotes, but it removed also
            // any quote at the end of the value
            // instead of trim(), we can just simply remove first and last character as it's always the extra quote
            'escape' => fn($value) => substr((string) json_encode($value), 1, -1),
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
