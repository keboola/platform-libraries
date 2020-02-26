<?php

namespace Keboola\InputMapping\Reader;

use Keboola\InputMapping\Exception\InvalidInputException;

class NullWorkspaceProvider implements WorkspaceProviderInterface
{
    public function getWorkspaceId($type)
    {
        throw new InvalidInputException(sprintf('Workspace "%s" is not available.', $type));
    }
}
