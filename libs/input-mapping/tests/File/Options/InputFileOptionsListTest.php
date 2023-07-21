<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\File\Options;

use Keboola\InputMapping\File\Options\InputFileOptions;
use Keboola\InputMapping\File\Options\InputFileOptionsList;
use PHPUnit\Framework\TestCase;

class InputFileOptionsListTest extends TestCase
{
    public function testGetFiles(): void
    {
        $definitions = new InputFileOptionsList(
            [
                ['tags' => ['foo']],
                ['tags' => ['bar']],
            ],
            false,
            '1234',
        );
        $files = $definitions->getFiles();
        self::assertCount(2, $files);
        self::assertSame(InputFileOptions::class, get_class($files[0]));
        self::assertSame(InputFileOptions::class, get_class($files[1]));
        self::assertSame(['foo'], $files[0]->getTags());
        self::assertSame(['bar'], $files[1]->getTags());
        self::assertFalse($files[0]->isDevBranch());
        self::assertFalse($files[1]->isDevBranch());
    }
}
