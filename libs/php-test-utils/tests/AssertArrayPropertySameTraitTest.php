<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests;

use Keboola\PhpTestUtils\AssertArrayPropertySameTrait;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\TestCase;

class AssertArrayPropertySameTraitTest extends TestCase
{
    use AssertArrayPropertySameTrait;

    public function testAssertArrayPropertySameTopLevel(): void
    {
        $row = [
            'id' => 123,
            'name' => 'Alice',
        ];

        self::assertArrayPropertySame(123, $row, 'id');
        self::assertArrayPropertySame('Alice', $row, 'name');
    }

    public function testAssertArrayPropertySameNested(): void
    {
        $row = [
            'customer' => [
                'id' => 'cust-001',
                'profile' => [
                    'age' => 34,
                ],
            ],
        ];

        self::assertArrayPropertySame('cust-001', $row, 'customer.id');
        self::assertArrayPropertySame(34, $row, 'customer.profile.age');
    }

    public function testFailsOnMismatch(): void
    {
        $row = [
            'customer' => [
                'id' => 'cust-001',
            ],
        ];

        $this->expectException(AssertionFailedError::class);
        self::assertArrayPropertySame('cust-XYZ', $row, 'customer.id');
    }

    public function testFailsWhenKeyMissing(): void
    {
        $row = [
            'customer' => [
                'id' => 'cust-001',
            ],
        ];

        $this->expectException(AssertionFailedError::class);
        self::assertArrayPropertySame('anything', $row, 'customer.email');
    }

    public function testFailsWhenArgNotArray(): void
    {
        $row = 'not-an-array';

        $this->expectException(AssertionFailedError::class);
        self::assertArrayPropertySame('value', $row, 'customer.id');
    }

    public function testFailsWhenIntermediateNotArray(): void
    {
        $row = [
            'customer' => 'not-an-array',
        ];

        $this->expectException(AssertionFailedError::class);
        self::assertArrayPropertySame('value', $row, 'customer.id');
    }

    public function testFailsWhenFinalNotScalar(): void
    {
        $row = [
            'customer' => [
                'profile' => [
                    'social' => [ 'twitter' => '@alice' ],
                ],
            ],
        ];

        $this->expectException(AssertionFailedError::class);
        self::assertArrayPropertySame('@alice', $row, 'customer.profile');
    }
}
