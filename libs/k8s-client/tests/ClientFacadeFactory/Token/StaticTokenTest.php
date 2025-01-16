<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\ClientFacadeFactory\Token;

use Keboola\K8sClient\ClientFacadeFactory\Token\StaticToken;
use PHPUnit\Framework\TestCase;

class StaticTokenTest extends TestCase
{
    public function testGetValue(): void
    {
        $token = new StaticToken('foo-token');

        self::assertSame('foo-token', $token->getValue());
    }
}
