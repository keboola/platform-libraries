<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

enum WorkspaceLoadType: string
{
    case CLONE = 'clone';    // Direct table clone from Table Storage to Workspace
    case COPY = 'copy';      // Copy table data from Table Storage to Workspace
    case VIEW = 'view';      // Create view in Workspace pointing to Table Storage
}
