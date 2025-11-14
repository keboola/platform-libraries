<?php

declare(strict_types=1);

namespace Keboola\StorageNamesSanitizer\Test;

use Generator;
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
     * @return Generator<string, array{string, string}>
     */
    public static function namesToSanitize(): Generator
    {
        yield '_~dlažební  %_kostky_~' => ['_~dlažební  %_kostky_~', 'dlazebni__kostky'];
        yield 'test-vn-đá cuội' => ['test-vn-đá cuội', 'test_vn_da_cuoi'];
        yield 'jp日本語' => ['jp日本語', 'jp'];
        // transliteratable emoji
        yield 'emoji 😀 name' => ['emoji 😀 name', 'emoji_name'];
        yield 'webalize | test 😁' => ['webalize | test 😁', 'webalize_test'];
        // non-transliteratable emoji
        yield 'ipsum | kockum 🐈' => ['ipsum | kockum 🐈', 'ipsum_kockum'];
        // Edge cases
        yield 'empty string' => ['', ''];
        yield 'spaces only' => ['   ', ''];
        yield 'numbers only' => ['123', '123'];
        yield 'underscore only' => ['_', ''];
        yield 'underscores around' => ['__test__', 'test'];
        // Valid characters boundary testing
        yield 'validName123' => ['validName123', 'validName123'];
        yield 'Valid_Name_123' => ['Valid_Name_123', 'Valid_Name_123'];
        yield 'test@domain.com' => ['test@domain.com', 'test_domain_com'];
        yield 'user-id#123' => ['user-id#123', 'user_id_123'];
        // Consecutive invalid characters
        yield 'test!!!multiple!!!invalid' => ['test!!!multiple!!!invalid', 'test_multiple_invalid'];
        yield 'name@@@with@@@symbols' => ['name@@@with@@@symbols', 'name_with_symbols'];
        // Leading underscore trimming
        yield '___leading_underscores' => ['___leading_underscores', 'leading_underscores'];
        yield '123number_start' => ['123number_start', '123number_start'];
        // Mixed cases
        yield 'CamelCase123' => ['CamelCase123', 'CamelCase123'];
        yield 'snake_case_valid' => ['snake_case_valid', 'snake_case_valid'];
    }

    /**
     * @dataProvider systemColumnsProvider
     */
    public function testSystemColumns(string $systemColumn, string $expected): void
    {
        $sanitized = ColumnNameSanitizer::sanitize($systemColumn);
        self::assertEquals($expected, $sanitized);
    }

    /**
     * @return Generator<string, array{string, string}>
     */
    public static function systemColumnsProvider(): Generator
    {
        yield 'oid lowercase' => ['oid', 'oid_'];
        yield 'OID uppercase' => ['OID', 'OID_'];
        yield 'Oid mixed' => ['Oid', 'Oid_'];
        yield 'tableoid lowercase' => ['tableoid', 'tableoid_'];
        yield 'TABLEOID uppercase' => ['TABLEOID', 'TABLEOID_'];
        yield 'TableOid mixed' => ['TableOid', 'TableOid_'];
        yield 'xmin lowercase' => ['xmin', 'xmin_'];
        yield 'XMIN uppercase' => ['XMIN', 'XMIN_'];
        yield 'Xmin mixed' => ['Xmin', 'Xmin_'];
        yield 'cmin lowercase' => ['cmin', 'cmin_'];
        yield 'CMIN uppercase' => ['CMIN', 'CMIN_'];
        yield 'Cmin mixed' => ['Cmin', 'Cmin_'];
        yield 'xmax lowercase' => ['xmax', 'xmax_'];
        yield 'XMAX uppercase' => ['XMAX', 'XMAX_'];
        yield 'Xmax mixed' => ['Xmax', 'Xmax_'];
        yield 'cmax lowercase' => ['cmax', 'cmax_'];
        yield 'CMAX uppercase' => ['CMAX', 'CMAX_'];
        yield 'Cmax mixed' => ['Cmax', 'Cmax_'];
        yield 'ctid lowercase' => ['ctid', 'ctid_'];
        yield 'CTID uppercase' => ['CTID', 'CTID_'];
        yield 'Ctid mixed' => ['Ctid', 'Ctid_'];
    }

    /**
     * @dataProvider lowerCaseProvider
     */
    public function testLowerCase(string $nameToSanitize, string $expected): void
    {
        $sanitized = ColumnNameSanitizer::sanitize(
            $nameToSanitize,
            ColumnNameSanitizer::REPLACE_INVALID_CHARACTERS_WITH,
            true,
        );
        self::assertEquals($expected, $sanitized);
    }

    /**
     * @return Generator<string, array{string, string}>
     */
    public static function lowerCaseProvider(): Generator
    {
        yield 'CamelCase123' => ['CamelCase123', 'camelcase123'];
        yield 'Valid_Name_123' => ['Valid_Name_123', 'valid_name_123'];
        yield 'TEST@DOMAIN.COM' => ['TEST@DOMAIN.COM', 'test_domain_com'];
        yield 'User-ID#123' => ['User-ID#123', 'user_id_123'];
        yield 'Mixed_Case_Test' => ['Mixed_Case_Test', 'mixed_case_test'];
        yield 'OID' => ['OID', 'oid_'];
        yield 'TABLEOID' => ['TABLEOID', 'tableoid_'];
    }

    /**
     * @dataProvider maxLengthProvider
     */
    public function testMaxLength(string $nameToSanitize, ?int $maxLength, string $expected): void
    {
        $sanitized = ColumnNameSanitizer::sanitize(
            $nameToSanitize,
            ColumnNameSanitizer::REPLACE_INVALID_CHARACTERS_WITH,
            false,
            $maxLength,
        );
        self::assertEquals($expected, $sanitized);
    }

    /**
     * @return Generator<string, array{string, int|null, string}>
     */
    public static function maxLengthProvider(): Generator
    {
        yield 'very long column name' => ['very_long_column_name_that_exceeds_limit', 10, 'very_long_'];
        yield 'test_column' => ['test_column', 5, 'test_'];
        yield 'short' => ['short', 10, 'short'];
        yield 'exactly_ten' => ['exactly_ten', 10, 'exactly_te'];
        yield 'exactly_eleven' => ['exactly_eleven', 11, 'exactly_ele'];
        yield 'test@domain.com' => ['test@domain.com', 8, 'test_dom'];
        yield 'oid' => ['oid', 3, 'oid_'];
        yield 'tableoid' => ['tableoid', 8, 'tableoid_'];
    }

    /**
     * @dataProvider replaceCharactersWithProvider
     */
    public function testReplaceCharactersWith(string $nameToSanitize, string $replaceWith, string $expected): void
    {
        $sanitized = ColumnNameSanitizer::sanitize($nameToSanitize, $replaceWith);
        self::assertEquals($expected, $sanitized);
    }

    /**
     * @return Generator<string, array{string, string, string}>
     */
    public static function replaceCharactersWithProvider(): Generator
    {
        yield 'test-column with X' => ['test-column', 'X', 'testXcolumn'];
        yield 'user@domain.com with -' => ['user@domain.com', '-', 'user-domain-com'];
        yield 'test!!!multiple with Z' => ['test!!!multiple', 'Z', 'testZmultiple'];
        yield 'name with spaces with .' => ['name with spaces', '.', 'name.with.spaces'];
        yield '_leading_underscore with X' => ['_leading_underscore', 'X', 'leading_underscore'];
        yield 'oid with X' => ['oid', 'X', 'oid_'];
    }

    /**
     * @dataProvider combinedParametersProvider
     */
    public function testCombinedParameters(
        string $nameToSanitize,
        string $replaceWith,
        bool $lower,
        ?int $maxLength,
        string $expected,
    ): void {
        $sanitized = ColumnNameSanitizer::sanitize($nameToSanitize, $replaceWith, $lower, $maxLength);
        self::assertEquals($expected, $sanitized);
    }

    /**
     * @return Generator<string, array{string, string, bool, int|null, string}>
     */
    public static function combinedParametersProvider(): Generator
    {
        yield 'CamelCase@Test with X, lower, maxLength 10' => ['CamelCase@Test', 'X', true, 10, 'camelcasex'];
        yield 'VeryLongColumnName with _, no lower, maxLength 8' => ['VeryLongColumnName', '_', false, 8, 'VeryLong'];
        yield 'Test@Domain.Com with -, lower, maxLength 12' => ['Test@Domain.Com', '-', true, 12, 'test-domain-'];
        yield 'OID with _, lower, maxLength 5' => ['OID', '_', true, 5, 'oid_'];
        yield 'TABLEOID with X, lower, maxLength 10' => ['TABLEOID', 'X', true, 10, 'tableoid_'];
    }
}
