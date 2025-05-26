<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Staging\File\LocalStaging;

use PHPUnit\Framework\TestCase;

class LocalStagingProviderTest extends TestCase
{
    public function testPathIsReturnedForLocalStaging(): void
    {
        $localPath = '/data/in/test';
        $workspaceProvider = new LocalStaging($localPath);

        self::assertSame($localPath, $workspaceProvider->getPath());
    }
}
