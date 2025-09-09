<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

enum WorkspaceJobType: string
{
    case CLONE = 'clone';     // Clone tables from Table Storage
    case LOAD = 'load';       // Load/copy tables from Table Storage
}
