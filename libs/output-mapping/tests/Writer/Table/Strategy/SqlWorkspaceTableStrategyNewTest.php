<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategyNew;
use PHPUnit\Framework\Assert;

class SqlWorkspaceTableStrategyNewTest extends AbstractWorkspaceTableStrategyNewTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->strategy = $this
            ->getWorkspaceStagingFactory($this->clientWrapper)
            ->getTableOutputStrategyNew(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);

        Assert::assertInstanceOf(SqlWorkspaceTableStrategyNew::class, $this->strategy);
    }
}
