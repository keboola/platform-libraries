<?php

namespace Keboola\OutputMapping\Tests\Writer\File;

use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Tests\Writer\BaseWriterWorkspaceTest;
use Keboola\OutputMapping\Writer\File\Strategy\Local;
use Keboola\OutputMapping\Writer\File\StrategyFactory;
use Keboola\StorageApi\Workspaces;
use Psr\Log\Test\TestLogger;

class StrategyFactoryTest extends BaseWriterWorkspaceTest
{
    use InitSynapseStorageClientTrait;

    /** @var array */
    private $workspace;

    protected function getWorkspaceProvider($workspaceData = [])
    {
        $mock = self::getMockBuilder(NullWorkspaceProvider::class)
            ->setMethods(['getCredentials'])
            ->getMock();
        $mock->method('getCredentials')->willReturnCallback(
            function ($type) use ($workspaceData) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $type]);
                    $this->workspace = $workspaceData ? $workspaceData : $workspace;
                }
                return $this->workspace['connection'];
            }
        );
        /** @var WorkspaceProviderInterface $mock */
        return $mock;
    }

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
        $strategy = new StrategyFactory(
            $this->clientWrapper,
            new TestLogger(),
            $this->getWorkspaceProvider(),
            '/data/out/files',
            'json'
        );
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_S3));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_ABS));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_LOCAL));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_SNOWFLAKE));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_REDSHIFT));
        self::assertInstanceOf(Local::class, $strategy->getStrategy(Reader::STAGING_SYNAPSE));
    }

    public function testGetStrategyInvalid()
    {
        $strategy = new StrategyFactory(
            $this->clientWrapper,
            new TestLogger(),
            $this->getWorkspaceProvider(),
            '/data/out/files',
            'json'
        );
        self::expectException(OutputOperationException::class);
        self::expectExceptionMessage(
            'FilesStrategy parameter "storageType" must be one of: s3, abs, workspace-redshift, workspace-snowflake,'
        );
        self::assertInstanceOf(Local::class, $strategy->getStrategy('invalid'));
    }
}
