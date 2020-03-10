<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;

class NullWorkspaceProviderTest extends \PHPUnit_Framework_TestCase
{
    public function testProvideSnowflakeWorkspace()
    {
        $provider = new NullWorkspaceProvider();
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Workspace "snowflake" is not available.');
        $provider->getWorkspaceId('snowflake');
    }

    public function testProvideSnowflakeWorkspaceCredentials()
    {
        $provider = new NullWorkspaceProvider();
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Workspace "snowflake" is not available.');
        $provider->getWorkspaceId('snowflake');
    }
}
