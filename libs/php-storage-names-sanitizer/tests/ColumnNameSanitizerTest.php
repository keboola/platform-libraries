<?php

declare(strict_types=1);

namespace Keboola\StorageNamesSanitizer\Test;

use Keboola\StorageNamesSanitizer\ColumnNameSanitizer;
use PHPUnit\Framework\TestCase;

class ColumnNameSanitizerTest extends TestCase
{
    /**
     * @dataProvider namesToSanitize
     **/
    public function testSanitizeColumnName(string $nameToSanitize, string $sanitizedName): void
    {
        $sanitized = ColumnNameSanitizer::sanitize($nameToSanitize);
        self::assertEquals($sanitizedName, $sanitized);
    }

    /**
     * @return array<array<string>>
     */
    public static function namesToSanitize(): array
    {
        return [
            [
                '_~dlažební  %_kostky_~',
                'dlazebni__kostky',
            ],
            [
                'test-vn-đá cuội',
                'test_vn_da_cuoi',
            ],
            [
                'jp日本語',
                'jp',
            ],
            // transliteratable emoji
            [
                'emoji 😀 name',
                'emoji_name',
            ],
            [
                'webalize | test 😁',
                'webalize_test',
            ],
            // non-transliteratable emoji
            [
                'ipsum | kockum 🐈',
                'ipsum_kockum',
            ],
            // Edge cases
            [
                '',
                '',
            ],
            [
                '   ',
                '',
            ],
            [
                '123',
                '123',
            ],
            [
                '_',
                '',
            ],
            [
                '__test__',
                'test',
            ],
            // Valid characters boundary testing
            [
                'validName123',
                'validName123',
            ],
            [
                'Valid_Name_123',
                'Valid_Name_123',
            ],
            [
                'test@domain.com',
                'test_domain_com',
            ],
            [
                'user-id#123',
                'user_id_123',
            ],
            // Consecutive invalid characters
            [
                'test!!!multiple!!!invalid',
                'test_multiple_invalid',
            ],
            [
                'name@@@with@@@symbols',
                'name_with_symbols',
            ],
            // Leading underscore trimming
            [
                '___leading_underscores',
                'leading_underscores',
            ],
            [
                '123number_start',
                '123number_start',
            ],
            // Mixed cases
            [
                'CamelCase123',
                'CamelCase123',
            ],
            [
                'snake_case_valid',
                'snake_case_valid',
            ],
        ];
    }
}
