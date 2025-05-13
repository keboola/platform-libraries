<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging;

enum StagingClass
{
    case File;
    case Workspace;
}
