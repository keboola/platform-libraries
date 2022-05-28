<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Configuration\Table\Manifest;

use Keboola\InputMapping\Configuration\Adapter as BaseAdapter;
use Keboola\InputMapping\Configuration\Table\Manifest;

class Adapter extends BaseAdapter
{
    protected string $configClass = Manifest::class;
}
