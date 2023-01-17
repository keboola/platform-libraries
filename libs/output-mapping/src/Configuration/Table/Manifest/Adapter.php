<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Configuration\Table\Manifest;

use Keboola\OutputMapping\Configuration\Adapter as ConfigurationAdapter;
use Keboola\OutputMapping\Configuration\Table\Manifest;

class Adapter extends ConfigurationAdapter
{
    protected string $configClass = Manifest::class;
}
