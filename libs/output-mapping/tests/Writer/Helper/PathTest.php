<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Writer\Helper\Path;
use PHPUnit\Framework\TestCase;

class PathTest extends TestCase
{
    /**
     * @dataProvider provideNoTrailingSlashTestData
     */
    public function testNoTrailingSlash(string $path, string $expectedResult): void
    {
        self::assertSame($expectedResult, Path::noTrailingSlash($path));
    }

    public function provideNoTrailingSlashTestData(): array
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

    /**
     * @dataProvider provideEnsureTrailingSlashTestData
     */
    public function testEnsureTrailingSlash(string $path, string $expectedResult): void
    {
        self::assertSame($expectedResult, Path::ensureTrailingSlash($path));
    }

    public function provideEnsureTrailingSlashTestData(): array
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

    /**
     * @dataProvider provideJoinTestData
     */
    public function testJoin(array $paths, string $expectedResult): void
    {
        self::assertSame($expectedResult, Path::join(...$paths));
    }

    public function provideJoinTestData(): array
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
}
