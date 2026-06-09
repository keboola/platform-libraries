<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Tests;

use Keboola\ApiClientBase\ResponseModelInterface;
use Keboola\ApiClientBase\Tests\Fixtures\DummyModel;
use PHPUnit\Framework\TestCase;

class ResponseModelInterfaceTest extends TestCase
{
    public function testFixtureImplementsContract(): void
    {
        $model = DummyModel::fromResponseData(['name' => 'foo']);
        self::assertInstanceOf(ResponseModelInterface::class, $model);
        self::assertSame('foo', $model->name);
    }
}
