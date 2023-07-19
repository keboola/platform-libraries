<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\File\Options;

use Keboola\InputMapping\File\Options\RewrittenInputFileOptions;
use Keboola\InputMapping\File\Options\RewrittenInputFileOptionsList;
use PHPUnit\Framework\TestCase;

class RewrittenInputFileOptionsListTest extends TestCase
{
    public function testGetFiles(): void
    {
        $definitions = new RewrittenInputFileOptionsList([
            new RewrittenInputFileOptions(['tags' => ['foo']], false, '1234', ['tags' => ['foo']], 123),
            new RewrittenInputFileOptions(['tags' => ['bar']], false, '1234', ['tags' => ['baz']], 345),
        ]);
        $files = $definitions->getFiles();
        self::assertCount(2, $files);
        self::assertSame(RewrittenInputFileOptions::class, get_class($files[0]));
        self::assertSame(RewrittenInputFileOptions::class, get_class($files[1]));
        self::assertSame(['foo'], $files[0]->getTags());
        self::assertSame(['bar'], $files[1]->getTags());
        self::assertFalse($files[0]->isDevBranch());
        self::assertFalse($files[1]->isDevBranch());
        self::assertSame([['name' => 'foo']], $files[0]->getFileConfigurationIdentifier());
        self::assertSame([['name' => 'baz']], $files[1]->getFileConfigurationIdentifier());
        self::assertSame(123, $files[0]->getSourceBranchId());
        self::assertSame(345, $files[1]->getSourceBranchId());
    }
}
