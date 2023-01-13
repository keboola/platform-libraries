<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Staging;

use Keboola\StagingProvider\Staging\LocalStaging;
use PHPUnit\Framework\TestCase;

class LocalStagingTest extends TestCase
{
    public function testCorrectTypeIsDefined(): void
    {
        self::assertSame('local', LocalStaging::getType());
    }

    public function testPathIsReturned(): void
    {
        $path = '/data/test';
        $workspace = new LocalStaging($path);

        self::assertSame($path, $workspace->getPath());
    }
}
