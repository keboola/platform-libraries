<?php

declare(strict_types=1);

namespace Keboola\LoggingBundle\Tests;

use Keboola\LoggingBundle\KeboolaLoggingBundle;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\MonologBundle\MonologBundle;
use Symfony\Component\Config\Loader\LoaderInterface;
use Symfony\Component\HttpKernel\Kernel;

class KeboolaLoggingBundleTestingKernel extends Kernel
{
    public function registerBundles(): iterable
    {
        yield new FrameworkBundle();
        yield new MonologBundle();
        yield new KeboolaLoggingBundle();
    }

    public function registerContainerConfiguration(LoaderInterface $loader): void
    {
        $loader->load(__DIR__.'/config.yaml');
    }
}
