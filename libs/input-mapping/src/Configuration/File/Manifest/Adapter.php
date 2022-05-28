<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Configuration\File\Manifest;

use Keboola\InputMapping\Configuration\Adapter as BaseAdapter;
use Keboola\InputMapping\Configuration\File\Manifest;

class Adapter extends BaseAdapter
{
    protected string $configClass = Manifest::class;
}
