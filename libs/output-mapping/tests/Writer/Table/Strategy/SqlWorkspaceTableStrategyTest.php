<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;

class SqlWorkspaceTableStrategyTest extends AbstractWorkspaceTableStrategyTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->strategy = $this
            ->getWorkspaceStagingFactory($this->clientWrapper)
            ->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);

        self::assertInstanceOf(SqlWorkspaceTableStrategy::class, $this->strategy);
    }
}
