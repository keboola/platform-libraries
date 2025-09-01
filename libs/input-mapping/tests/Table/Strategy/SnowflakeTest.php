<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\Snowflake;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Psr\Log\NullLogger;

class SnowflakeTest extends AbstractTestCase
{
    public function testGetWorkspaceType(): void
    {
        $strategy = new Snowflake(
            $this->initClient(),
            new NullLogger(),
            $this->createMock(WorkspaceStagingInterface::class),
            $this->createMock(FileStagingInterface::class),
            new InputTableStateList([]),
            'test',
            FileFormat::Json,
        );

        self::assertEquals('snowflake', $strategy->getWorkspaceType());
    }
}
