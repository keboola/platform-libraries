<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesRenderer;

use Keboola\ConfigurationVariablesResolver\VariablesRenderer\RegexRenderer;
use PHPUnit\Framework\TestCase;

class RegexRendererTest extends TestCase
{
    public function testRenderVariableWithoutPrefix(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ key1 }}, prefixed key1 is {{ prefix.key1 }}',
                ],
            ],
            '',
            [
                'key1' => 'value1',
            ],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is value1, prefixed key1 is {{ prefix.key1 }}',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['key1'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderVariableWithPrefix(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ key1 }}, prefixed key1 is {{ prefix.key1 }}',
                ],
            ],
            'prefix',
            [
                'key1' => 'value1',
            ],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is {{ key1 }}, prefixed key1 is value1',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['prefix.key1'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderVariableWithSpecialCharacters(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ key1 }}',
                ],
            ],
            '',
            [
                'key1' => '\' { @ "',
            ],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is \' { @ "',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['key1'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderMissingVariable(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ prefix.key1 }}',
                ],
            ],
            'prefix',
            [],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is {{ prefix.key1 }}',
                ],
            ],
            $results->configuration,
        );
        self::assertSame([], $results->replacedVariables);
        self::assertSame(['prefix.key1'], $results->missingVariables);
    }

    public function testRenderMultipleVariableOccurrences(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ key1 }} and also {{ key1 }}',
                ],
            ],
            '',
            [
                'key1' => 'value1',
            ],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is value1 and also value1',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['key1'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderTriplePlaceholder(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{{ key1 }}}',
                ],
            ],
            '',
            [
                'key1' => 'value1',
            ],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is {{{ key1 }}}',
                ],
            ],
            $results->configuration,
        );
        self::assertSame([], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderVariablesWithVariousWhitespaces(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{key1}} and {{   key1  }} and {{  key1}}',
                ],
            ],
            '',
            [
                'key1' => 'value1',
            ],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is value1 and value1 and value1',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['key1'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderNumericVariable(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ 123 }}',
                ],
            ],
            '',
            [
                '123' => 'value1',
            ],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is value1',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['123'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testRenderInvalidVariable(): void
    {
        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ not@valid }}',
                ],
            ],
            '',
            [
                'not@valid' => 'value1',
            ],
        );

        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is {{ not@valid }}',
                ],
            ],
            $results->configuration,
        );
        self::assertSame([], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testLazyLoadVariables(): void
    {
        $invocationCount = 0;

        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ key1 }}, key2 is {{ key2 }}',
                ],
            ],
            '',
            function () use (&$invocationCount) {
                $invocationCount += 1;
                return [
                    'key1' => 'value1',
                    'key2' => 'value2',
                ];
            },
        );

        self::assertSame(1, $invocationCount);
        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is value1, key2 is value2',
                ],
            ],
            $results->configuration,
        );
        self::assertSame(['key1', 'key2'], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }

    public function testLazyLoadVariablesIsNotTriggeredWhenNoVariableIsPresent(): void
    {
        $invocationCount = 0;

        $renderer = new RegexRenderer();
        $results = $renderer->renderVariables(
            [
                'parameters' => [
                    'param' => 'key1 is {{ other.key1 }}',
                ],
            ],
            '',
            function () use (&$invocationCount) {
                $invocationCount += 1;
                return [];
            },
        );

        self::assertSame(0, $invocationCount);
        self::assertEquals(
            [
                'parameters' => [
                    'param' => 'key1 is {{ other.key1 }}',
                ],
            ],
            $results->configuration,
        );
        self::assertSame([], $results->replacedVariables);
        self::assertSame([], $results->missingVariables);
    }
}
