<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy;

class SqlWorkspaceTableStrategyTest extends AbstractWorkspaceTableStrategyTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->strategy = $this
            ->getWorkspaceStagingFactory(
                clientWrapper: $this->clientWrapper,
                workspaceId: 'fake-workspace-id',
            )
            ->getTableOutputStrategy();

        self::assertInstanceOf(SqlWorkspaceTableStrategy::class, $this->strategy);
    }
}
