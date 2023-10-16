<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests;

use AymDev\MessengerAzureBundle\AymDevMessengerAzureBundle;
use Keboola\MessengerBundle\KeboolaMessengerBundle;
use PetitPress\GpsMessengerBundle\GpsMessengerBundle;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\Config\Loader\LoaderInterface;
use Symfony\Component\HttpKernel\Kernel;

class KeboolaMessengerBundleTestingKernel extends Kernel
{
    public function registerBundles(): iterable
    {
        yield new AymDevMessengerAzureBundle();
        yield new GpsMessengerBundle();
        yield new FrameworkBundle();
        yield new KeboolaMessengerBundle();
    }

    public function registerContainerConfiguration(LoaderInterface $loader): void
    {
        $loader->load(__DIR__.'/config.yaml');
    }
}
