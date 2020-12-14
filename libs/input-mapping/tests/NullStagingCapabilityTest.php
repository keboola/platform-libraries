<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\NullCapability;

class NullStagingCapabilityTest extends \PHPUnit_Framework_TestCase
{
    public function testProvideSnowflakeWorkspace()
    {
        $provider = new NullCapability();
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Workspace "snowflake" is not available.');
        $provider->getWorkspaceId('snowflake');
    }

    public function testProvideSnowflakeWorkspaceCredentials()
    {
        $provider = new NullCapability();
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Workspace "snowflake" is not available.');
        $provider->getWorkspaceId('snowflake');
    }
}
