<?php

namespace Keboola\OutputMapping\Configuration\Table\Manifest;

use Keboola\DockerBundle\Docker\Configuration;

class Adapter extends Configuration\Adapter
{
    protected $configClass = Configuration\Output\Table\Manifest::class;
}
