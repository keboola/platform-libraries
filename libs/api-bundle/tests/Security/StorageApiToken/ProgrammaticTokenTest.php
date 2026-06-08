<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Generator;
use Keboola\ApiBundle\Security\StorageApiToken\ProgrammaticToken;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class ProgrammaticTokenTest extends TestCase
{
    #[DataProvider('provideTokens')]
    public function testMatches(string $token, bool $expected): void
    {
        self::assertSame($expected, ProgrammaticToken::matches($token));
    }

    public static function provideTokens(): Generator
    {
        yield 'access token' => ['token' => 'kbc_at_abc123', 'expected' => true];
        yield 'personal access token' => ['token' => 'kbc_pat_abc123', 'expected' => true];
        yield 'access token prefix only' => ['token' => 'kbc_at_', 'expected' => true];
        yield 'legacy storage token' => ['token' => '1234-abcdefghij', 'expected' => false];
        yield 'empty token' => ['token' => '', 'expected' => false];
        yield 'prefix in the middle' => ['token' => 'x-kbc_at_abc', 'expected' => false];
        yield 'similar but different prefix' => ['token' => 'kbc_a_abc', 'expected' => false];
    }
}
