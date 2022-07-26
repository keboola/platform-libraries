<?php

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Helper\Path;
use PHPUnit\Framework\TestCase;

class PathTest extends TestCase
{
    /**
     * @dataProvider provideNoTrailingSlashTestData
     */
    public function testNoTrailingSlash($path, $expectedResult)
    {
        self::assertSame($expectedResult, Path::noTrailingSlash($path));
    }

    public function provideNoTrailingSlashTestData()
    {
        return [
            ['', ''],
            ['/', '/'],
            ['/aa', '/aa'],
            ['/aa/', '/aa'],
            ['/aa//xx', '/aa//xx'],
            ['/aa//xx//', '/aa//xx'],
        ];
    }

    public function testNoTrailingSlashRequiresString()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $path must be a string, boolean given');

        Path::noTrailingSlash(false);
    }

    /**
     * @dataProvider provideEnsureTrailingSlashTestData
     */
    public function testEnsureTrailingSlash($path, $expectedResult)
    {
        self::assertSame($expectedResult, Path::ensureTrailingSlash($path));
    }

    public function provideEnsureTrailingSlashTestData()
    {
        return [
            ['', ''],
            ['/', '/'],
            ['/aa', '/aa/'],
            ['/aa/', '/aa/'],
            ['/aa//xx', '/aa//xx/'],
            ['/aa//xx//', '/aa//xx/'],
        ];
    }

    public function testEnsureTrailingSlashRequiresString()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $path must be a string, boolean given');

        Path::ensureTrailingSlash(false);
    }

    /**
     * @dataProvider provideJoinTestData
     */
    public function testJoin($paths, $expectedResult)
    {
        self::assertSame($expectedResult, Path::join(...$paths));
    }

    public function provideJoinTestData()
    {
        return [
            [[], ''],
            [[''], ''],
            [['/'], '/'],
            [['aa', 'bb'], 'aa/bb'],
            [['aa', '/bb'], 'aa/bb'],
            [['/aa', 'bb'], '/aa/bb'],
            [['aa/', 'bb'], 'aa/bb'],
            [['aa/', 'bb/'], 'aa/bb'],
            [['aa', '//bb'], 'aa/bb'],
            [['aa', '/', 'bb'], 'aa/bb'],
            [['', 'aa', 'bb'], 'aa/bb'],
            [['aa', '', 'bb'], 'aa/bb'],
            [['aa', 'bb', ''], 'aa/bb'],
        ];
    }

    public function testJoinRequiresStringParts()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $path must be a string, integer given');

        Path::join(1);
    }
}
