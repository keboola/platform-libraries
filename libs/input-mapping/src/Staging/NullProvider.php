<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use LogicException;

class NullProvider implements ProviderInterface
{
    public function getWorkspaceId(): string
    {
        throw new LogicException('getWorkspaceId not implemented.');
    }

    public function cleanup(): void
    {
    }

    public function getCredentials(): array
    {
        return [];
    }

    public function getPath(): string
    {
        throw new LogicException('getPath not implemented.');
    }
}
