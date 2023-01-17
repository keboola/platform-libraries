<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\File\Manifest;

use Keboola\OutputMapping\Configuration\Adapter as ConfigurationAdapter;
use Keboola\OutputMapping\Configuration\File\Manifest;

class Adapter extends ConfigurationAdapter
{
    protected string $configClass = Manifest::class;
}
