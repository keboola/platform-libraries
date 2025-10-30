<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

enum WorkspaceLoadType: string
{
    case CLONE = 'CLONE';    // Direct table clone from Table Storage to Workspace
    case COPY = 'COPY';      // Copy table data from Table Storage to Workspace
    case VIEW = 'VIEW';      // Create view in Workspace pointing to Table Storage
}
