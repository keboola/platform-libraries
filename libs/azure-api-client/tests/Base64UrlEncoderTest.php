<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use Generator;
use Keboola\AzureApiClient\Base64UrlEncoder;
use PHPUnit\Framework\TestCase;

class Base64UrlEncoderTest extends TestCase
{
    /**
     * @dataProvider paddingStringProvider
     */
    public function testEncoder(string $string, string $encoded): void
    {
        $result = Base64UrlEncoder::encode($string);
        self::assertEquals($encoded, $result);
        self::assertEquals($string, Base64UrlEncoder::decode($result));
    }

    public function paddingStringProvider(): Generator
    {
        yield [
            ')_+\\(*&^%$#@!)=/"\'junk',
            'KV8rXCgqJl4lJCNAISk9LyInanVuaw',
        ];
        yield [
            '+',
            'Kw',
        ];
        yield [
            '++',
            'Kys',
        ];
        yield [
            '+++',
            'Kysr',
        ];
        yield [
            '++++',
            'KysrKw',
        ];
        yield [
            '+++++',
            'KysrKys',
        ];
    }
}
