<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Provider;

use Keboola\StagingProvider\Provider\LocalStagingProvider;

use PHPUnit\Framework\TestCase;

class LocalStagingProviderTest extends TestCase
{
    public function testPathIsReturnedForLocalStaging(): void
    {
        $localPath = '/data/in/test';
        $workspaceProvider = new LocalStagingProvider($localPath);

        self::assertSame($localPath, $workspaceProvider->getPath());
    }
}
