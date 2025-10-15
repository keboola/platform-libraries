<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests;

use Keboola\PhpTestUtils\AssertObjectPropertyValueTrait;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\TestCase;

class AssertObjectPropertyValueTraitTest extends TestCase
{
    use AssertObjectPropertyValueTrait;

    public function testAssertObjectPropertyValueMatches(): void
    {
        $client = new class('https://example.com') {
            // @phpstan-ignore-next-line
            private string $url;
            public function __construct(string $url)
            {
                $this->url = $url;
            }
        };
        self::assertObjectPropertyValue('https://example.com', $client, 'url');
    }

    public function testAssertObjectPropertyValueFailsOnMismatch(): void
    {
        $client = new class('https://example.com') {
            // @phpstan-ignore-next-line
            private string $url;
            public function __construct(string $url)
            {
                $this->url = $url;
            }
        };

        $this->expectException(AssertionFailedError::class);
        self::assertObjectPropertyValue('https://different.test', $client, 'url');
    }
}
