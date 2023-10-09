<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle;

use Keboola\MessengerBundle\DependencyInjection\KeboolaMessengerExtension;
use Symfony\Component\DependencyInjection\Extension\ExtensionInterface;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;

class KeboolaMessengerBundle extends AbstractBundle
{
    public function getContainerExtension(): ?ExtensionInterface
    {
        return new KeboolaMessengerExtension();
    }
}
