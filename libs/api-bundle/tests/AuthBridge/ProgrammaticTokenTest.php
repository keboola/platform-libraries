<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\AuthBridge;

use Generator;
use Keboola\ApiBundle\AuthBridge\ProgrammaticToken;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class ProgrammaticTokenTest extends TestCase
{
    #[DataProvider('provideTokenMatchCases')]
    public function testMatches(string $token, bool $expected): void
    {
        self::assertSame($expected, ProgrammaticToken::matches($token));
    }

    public static function provideTokenMatchCases(): Generator
    {
        yield 'kbc_at_ with payload' => [
            'token' => 'kbc_at_abc123',
            'expected' => true,
        ];

        yield 'kbc_pat_ with payload' => [
            'token' => 'kbc_pat_xyz789',
            'expected' => true,
        ];

        yield 'exact kbc_at_ prefix only' => [
            'token' => 'kbc_at_',
            'expected' => true,
        ];

        yield 'exact kbc_pat_ prefix only' => [
            'token' => 'kbc_pat_',
            'expected' => true,
        ];

        yield 'legacy storage token with dashes' => [
            'token' => '12345-storagetoken',
            'expected' => false,
        ];

        yield 'kbc_ prefix but unknown variant' => [
            'token' => 'kbc_other_token',
            'expected' => false,
        ];

        yield 'empty string' => [
            'token' => '',
            'expected' => false,
        ];

        yield 'Bearer prefixed kbc_at token' => [
            'token' => 'Bearer kbc_at_token',
            'expected' => false,
        ];

        yield 'uppercase variant does not match' => [
            'token' => 'KBC_AT_token',
            'expected' => false,
        ];
    }
}
