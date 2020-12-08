<?php

namespace Keboola\OutputMapping\Tests\Writer\File;

use Keboola\InputMapping\Reader\Reader;
use Keboola\OutputMapping\Tests\Writer\AbsWriterWorkspaceTest;
use Keboola\OutputMapping\Writer\File\Strategy\ABSWorkspace;
use Keboola\OutputMapping\Writer\File\Strategy\Local;
use Keboola\OutputMapping\Writer\File\StrategyFactory;
use Psr\Log\Test\TestLogger;

class StrategyFactorySynapseTest extends AbsWriterWorkspaceTest
{
    protected function initClient()
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    public function setUp()
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        parent::setUp();
    }

    public function testGetStrategy()
    {
        $strategy = new StrategyFactory($this->clientWrapper, new TestLogger(), $this->getWorkspaceProvider(), '/data/out/files', 'json');
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_S3));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_ABS));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_LOCAL));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_SNOWFLAKE));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_REDSHIFT));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_SYNAPSE));
        self::assertInstanceOf(ABSWorkspace::class, $strategy->getStrategy(Reader::STAGING_ABS_WORKSPACE));
    }
}
