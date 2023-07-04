<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\VariablesRenderer;

use Keboola\ConfigurationVariablesResolver\VariablesRenderer\VariablesContext;
use PHPUnit\Framework\TestCase;

class VariablesContextTest extends TestCase
{
    public function testContext(): void
    {
        $context = new VariablesContext([
            'a' => 'b',
            'c' => 'd',
            'nested' => [
                'a' => 'c',
            ],
        ]);

        self::assertTrue(isset($context->a));
        self::assertTrue(isset($context->c));
        self::assertFalse(isset($context->e));
        self::assertTrue(isset($context->nested));
        self::assertTrue(isset($context->nested->a));
        self::assertFalse(isset($context->nested->b));

        self::assertSame('b', $context->a);
        self::assertSame('d', $context->c);
        self::assertSame('c', $context->nested->a);

        self::assertSame(['nested.a', 'a', 'c'], $context->getReplacedVariables());
        self::assertSame(['e', 'nested.b'], $context->getMissingVariables());
    }
}
