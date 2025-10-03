<?php

declare(strict_types=1);

namespace Keboola\Utils\Sanitizer\Test;

use Keboola\Utils\Sanitizer\ColumnNameSanitizer;
use PHPUnit\Framework\TestCase;

class ColumnNameSanitizerTest extends TestCase
{
//    /**
//     * @dataProvider testStrings
//     **/
//    public function testToAscii($testString, $expectedAscii): void
//    {
//        $asciid = ColumnNameSanitizer::toAscii($testString);
//        self::assertEquals($expectedAscii, $asciid);
//    }
//
//    public function testStrings()
//    {
//        return [
//            [
//                '_~dlažební  %_kostky_~',
//                '_~dlazebni  %_kostky_~',
//            ],[
//                'test-vn-đá cuội',
//                'test-vn-da cuoi',
//            ],[
//                'jp日本語',
//                'jp???',
//            ],
//        ];
//    }

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
            [
                'emoji😀name',
                'emoji_name',
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
