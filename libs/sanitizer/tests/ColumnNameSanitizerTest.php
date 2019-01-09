<?php

namespace Keboola\Utils\Sanitizer\Test;

use Keboola\Utils\Sanitizer\ColumnNameSanitizer;
use PHPUnit\Framework\TestCase;

class ColumnNameSanitizerTest extends TestCase
{
    /**
     * @dataProvider testStrings
     **/
    public function testToAscii($testString, $expectedAscii)
    {
        $asciid = ColumnNameSanitizer::toAscii($testString);
        self::assertEquals($expectedAscii, $asciid);
    }

    public function testStrings()
    {
        return [
            [
                "_~dlažební  %_kostky_~",
                "_~dlazebni  %_kostky_~"
            ],[
                "test-vn-đá cuội",
                "test-vn-da cuoi"
            ],[
                "jp日本語",
                "jp???"
            ]
        ];
    }

    /**
     * @dataProvider namesToSanitize
     **/
    public function testSanitizeColumnName($nameToSanitize, $sanitizedName)
    {
        $sanitized = ColumnNameSanitizer::sanitize($nameToSanitize);
        self::assertEquals($sanitizedName, $sanitized);
    }

    public function namesToSanitize()
    {
        return [
            [
                "_~dlažební  %_kostky_~",
                "dlazebni__kostky"
            ],[
                "test-vn-đá cuội",
                "test_vn_da_cuoi"
            ],[
                "jp日本語",
                "jp"
            ]
        ];
    }
}
