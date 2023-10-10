<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesRenderer;

use Keboola\ConfigurationVariablesResolver\VariablesRenderer\MustacheVariablesContext;
use PHPUnit\Framework\TestCase;

class MustacheVariablesContextTest extends TestCase
{
    public function testContext(): void
    {
        $context = new MustacheVariablesContext([
            'a' => 'b',
            'c' => 'd',
        ]);

        self::assertTrue(isset($context->a));
        self::assertTrue(isset($context->c));
        self::assertFalse(isset($context->e));

        self::assertSame('b', $context->a);
        self::assertSame('d', $context->c);

        self::assertSame(
            [
                'a' => 'b',
                'c' => 'd',
            ],
            $context->getReplacedVariablesValues(),
        );
        self::assertSame(['a', 'c'], $context->getReplacedVariables());
        self::assertSame(['e'], $context->getMissingVariables());
    }
}
