<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;

class TestWorkspaceStrategy extends AbstractWorkspaceStrategy
{
    private string $workspaceType;

    public function setWorkspaceType(string $workspaceType): void
    {
        $this->workspaceType = $workspaceType;
    }

    public function getWorkspaceType(): string
    {
        return $this->workspaceType;
    }
}
