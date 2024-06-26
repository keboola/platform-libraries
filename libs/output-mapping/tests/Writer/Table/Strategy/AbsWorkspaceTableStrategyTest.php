<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;

class AbsWorkspaceTableStrategyTest extends AbstractWorkspaceTableStrategyTestCase
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
            ->getTableOutputStrategy(AbstractStrategyFactory::WORKSPACE_ABS);
    }
}
