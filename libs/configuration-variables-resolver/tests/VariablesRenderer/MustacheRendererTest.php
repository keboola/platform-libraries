<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesRenderer;

use Keboola\ConfigurationVariablesResolver\Exception\UserException;
use Keboola\ConfigurationVariablesResolver\VariablesRenderer\MustacheRenderer;
use PHPUnit\Framework\TestCase;

class MustacheRendererTest extends TestCase
{
    public function testRenderVariables(): void
    {
        $renderer = new MustacheRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'foo is {{ foo }}, goo is {{ goo }}',
                ],
            ],
            [
                'foo' => 'bar',
                'goo' => 'gar',
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'foo is bar, goo is gar',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['foo', 'goo'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderSingleVariableMultipleTimes(): void
    {
        $renderer = new MustacheRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'foo is {{ foo }} and {{ foo }}',
                ],
            ],
            [
                'foo' => 'bar',
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'foo is bar and bar',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['foo'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderNestedVariables(): void
    {
        $renderer = new MustacheRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'global key1: {{ key1 }}, vault key1: {{ vault.key1 }}',
                ],
            ],
            [
                'key1' => 'val1',
                'vault' => [
                    'key1' => 'val2',
                ],
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'global key1: val1, vault key1: val2',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['key1', 'vault.key1'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderMissingVariable(): void
    {
        $renderer = new MustacheRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => '{{ key1 }} {{ key2 }} {{ vault.key1 }}',
                ],
            ],
            [
                'key1' => 'val1',
                'vault' => [],
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'val1  ',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['key1'], $results->replacedVariables);
        self::assertSame(['key2', 'vault.key1'], $results->missingVariables);
    }

    public function testRenderVariablesSpecialCharacterReplacement(): void
    {
        $renderer = new MustacheRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'foo is {{ foo }}',
                ],
            ],
            [
                'foo' => 'special " \' { } characters',
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => 'foo is special " \' { } characters',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['foo'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderValueEndingWithQuote(): void
    {
        $renderer = new MustacheRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => '{{ key1 }}',
                ],
            ],
            [
                'key1' => '"',
            ]
        );

        self::assertSame(
            [
                'parameters' => [
                    'param' => '"',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['key1'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderJsonBreakingValue(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Variable replacement resulted in invalid configuration, error: ' .
            'Control character error, possibly incorrectly encoded',
        );

        $renderer = new MustacheRenderer();
        $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => '{{{ key1 }}}',
                ],
            ],
            [
                'key1' => 'value"',
            ]
        );
    }
}
