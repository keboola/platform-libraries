<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Writer\Table\Strategy\AbsWorkspaceTableStrategyNew;

class AbsWorkspaceTableStrategyNewTest extends AbstractWorkspaceTableStrategyNewTestCase
{
    use InitSynapseStorageClientTrait;

    public function setUp(): void
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();

        $this->temp = $this->createTemp();

        $this->strategy = $this
            ->getWorkspaceStagingFactory(
                $this->clientWrapper,
                'json',
                null,
                [AbstractStrategyFactory::WORKSPACE_ABS, 'abs'],
            )
            ->getTableOutputStrategyNew(AbstractStrategyFactory::WORKSPACE_ABS);

        self::assertInstanceOf(AbsWorkspaceTableStrategyNew::class, $this->strategy);
    }
}
