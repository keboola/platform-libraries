<?php

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Staging\NullProvider;
use LogicException;
use PHPUnit\Framework\TestCase;

class NullProviderTest extends TestCase
{
    public function testProvideSnowflakeWorkspace()
    {
        $provider = new NullProvider();
        $provider->cleanup();
        self::assertSame([], $provider->getCredentials());
        self::expectException(LogicException::class);
        self::expectExceptionMessage('getWorkspaceId not implemented.');
        $provider->getWorkspaceId();
    }

    public function testProvideSnowflakeWorkspacePath()
    {
        $provider = new NullProvider();
        self::expectException(LogicException::class);
        self::expectExceptionMessage('getPath not implemented.');
        $provider->getPath();
    }
}
