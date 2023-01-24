<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Staging\NullProvider;
use LogicException;
use PHPUnit\Framework\TestCase;

class NullProviderTest extends TestCase
{
    public function testProvideSnowflakeWorkspace(): void
    {
        $provider = new NullProvider();
        $provider->cleanup();
        self::assertSame([], $provider->getCredentials());
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('getWorkspaceId not implemented.');
        $provider->getWorkspaceId();
    }

    public function testProvideSnowflakeWorkspacePath(): void
    {
        $provider = new NullProvider();
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('getPath not implemented.');
        $provider->getPath();
    }
}
