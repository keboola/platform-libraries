<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

use JsonException;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Mustache_Engine;
use Psr\Log\LoggerInterface;

class MustacheRenderer
{
    private readonly Mustache_Engine $mustache;

    public function __construct()
    {
        $this->mustache = new Mustache_Engine([
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
    public function renderVariables(array $configuration, array $variables): RenderResults
    {
        $context = new MustacheVariablesContext($variables);

        $renderedConfiguration = $this->mustache->render(
            (string) json_encode($configuration, JSON_THROW_ON_ERROR),
            $context,
        );

        try {
            $configuration = (array) json_decode($renderedConfiguration, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new UserException(
                'Variable replacement resulted in invalid configuration, error: ' . $e->getMessage(),
            );
        }

        return new RenderResults(
            $configuration,
            $context->getReplacedVariables(),
            $context->getReplacedVariablesValues(),
            $context->getMissingVariables(),
        );
    }
}
