<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Source;

enum SourceType: string
{
    case WORKSPACE = 'workspace';
    case LOCAL = 'local';
    case FILE = 'file';
}
