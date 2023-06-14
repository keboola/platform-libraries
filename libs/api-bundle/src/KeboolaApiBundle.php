<?php

declare(strict_types=1);

namespace Keboola\ApiBundle;

use Keboola\ApiBundle\DependencyInjection\KeboolaApiExtension;
use Symfony\Component\DependencyInjection\Extension\ExtensionInterface;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;

class KeboolaApiBundle extends AbstractBundle
{
    public function getContainerExtension(): ?ExtensionInterface
    {
        return new KeboolaApiExtension();
    }
}
