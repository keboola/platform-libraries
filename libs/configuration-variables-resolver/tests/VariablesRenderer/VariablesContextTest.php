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
        ]);

        self::assertTrue(isset($context->a));
        self::assertTrue(isset($context->c));
        self::assertFalse(isset($context->e));

        self::assertSame('b', $context->a);
        self::assertSame('d', $context->c);

        self::assertSame(['a', 'c'], $context->getReplacedVariables());
        self::assertSame(['e'], $context->getMissingVariables());
    }
}
