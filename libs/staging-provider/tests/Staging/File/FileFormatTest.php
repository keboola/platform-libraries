<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Staging\File;

use Keboola\StagingProvider\Staging\File\FileFormat;
use PHPUnit\Framework\TestCase;

class FileFormatTest extends TestCase
{
    public function provideFileFormatsExtensions(): iterable
    {
        yield 'json' => [
            'fileFormat' => FileFormat::Json,
            'expectedExtension' => '.json',
        ];

        yield 'yaml' => [
            'fileFormat' => FileFormat::Yaml,
            'expectedExtension' => '.yml',
        ];
    }

    /** @dataProvider provideFileFormatsExtensions */
    public function testGetFileExtension(FileFormat $fileFormat, string $expectedExtension): void
    {
        self::assertSame($expectedExtension, $fileFormat->getFileExtension());
    }
}
