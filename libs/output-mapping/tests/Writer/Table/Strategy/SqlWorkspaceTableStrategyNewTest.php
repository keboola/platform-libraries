<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;

class SqlWorkspaceTableStrategyNewTest extends WorkspaceTableStrategyNewTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->strategy = $this
            ->getWorkspaceStagingFactory($this->clientWrapper)
            ->getTableOutputStrategyNew(AbstractStrategyFactory::WORKSPACE_SNOWFLAKE);
    }
}
