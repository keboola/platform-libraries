<?php

namespace Keboola\WorkspaceProvider\Tests\Staging;

use Keboola\WorkspaceProvider\Staging\LocalStaging;
use PHPUnit\Framework\TestCase;

class LocalStagingTest extends TestCase
{
    public function testCorrectTypeIsDefined()
    {
        self::assertSame('local', LocalStaging::getType());
    }

    public function testPathIsReturned()
    {
        $path = '/data/test';
        $workspace = new LocalStaging($path);

        self::assertSame($path, $workspace->getPath());
    }
}
