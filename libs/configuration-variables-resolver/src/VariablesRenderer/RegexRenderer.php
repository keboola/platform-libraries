<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\VariablesRenderer;

use JsonException;
use Keboola\ConfigurationVariablesResolver\Exception\UserException;

class RegexRenderer
{
    private const PREFIX_SEPARATOR = '.';

    /**
     * @param callable(): array<string> $valuesLoader
     */
    public function renderVariables(array $configuration, string $namePrefix, callable $valuesLoader): RenderResults
    {
        if ($namePrefix !== '') {
            $namePrefix = $namePrefix . self::PREFIX_SEPARATOR;
        }

        $values = null;
        $missingVariables = [];
        $replacedVariables = [];

        $renderedString = preg_replace_callback(
            // (?<!{) - do not match more than two opening braces
            // {{\s* - match opening braces and optional whitespaces
            // %s[a-zA-Z0-9_-]+ - match variable name (with prefix if supplied)
            // \s*}} - match optional whitespaces and closing braces
            sprintf('/(?<!{){{\s*(%s([a-zA-Z0-9_-]+))\s*}}/', preg_quote($namePrefix, '/')),
            function (array $match) use (&$values, $valuesLoader, &$missingVariables, &$replacedVariables): string {
                [$placeholder, $prefixedName, $localName] = $match;

                if ($values === null) {
                    $values = $valuesLoader();
                }

                if (!isset($values[$localName])) {
                    $missingVariables[$prefixedName] = true;
                    return $placeholder;
                }

                $replacedVariables[$prefixedName] = true;
                $value = $values[$localName];

                return self::escapeValue($value);
            },
            json_encode($configuration, JSON_THROW_ON_ERROR),
        );

        try {
            $configuration = (array) json_decode((string) $renderedString, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $e) {
            throw new UserException(
                'Variable replacement resulted in invalid configuration, error: ' . $e->getMessage()
            );
        }

        return new RenderResults(
            $configuration,
            self::mapVariablesList($replacedVariables),
            self::mapVariablesList($missingVariables),
        );
    }

    private static function escapeValue(string $value): string
    {
        return substr((string) json_encode($value), 1, -1);
    }

    /**
     * @return list<non-empty-string>
     */
    private static function mapVariablesList(array $variables): array
    {
        return array_map(strval(...), array_keys($variables)); // @phpstan-ignore-line
    }
}
