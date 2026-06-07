<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\DependencyInjection;

use Keboola\MessengerBundle\KeboolaMessengerBundle;
use Keboola\MessengerBundle\Transport\GpsTransportFactoryDecorator;
use PetitPress\GpsMessengerBundle\Transport\GpsTransportFactory;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Symfony\Component\Config\Loader\LoaderInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Kernel;

class GpsTransportFactoryDecorationTest extends KernelTestCase
{
    public function testGpsTransportFactoryIsDecoratedWhenGpsMessengerBundleIsRegistered(): void
    {
        self::bootKernel();

        $factory = self::getContainer()->get(GpsTransportFactory::class);
        self::assertInstanceOf(GpsTransportFactoryDecorator::class, $factory);
    }

    public function testContainerCompilesWhenGpsMessengerBundleIsNotRegistered(): void
    {
        $kernel = new class('test', true) extends Kernel {
            public function registerBundles(): iterable
            {
                yield new FrameworkBundle();
                yield new KeboolaMessengerBundle();
            }

            public function registerContainerConfiguration(LoaderInterface $loader): void
            {
                $loader->load(function (ContainerBuilder $container): void {
                    $container->loadFromExtension('framework', [
                        'test' => true,
                        'http_method_override' => false,
                    ]);
                });
            }
        };

        $kernel->boot();
        $container = $kernel->getContainer();

        self::assertFalse($container->has(GpsTransportFactory::class));
        self::assertFalse($container->has('keboola.messenger_bundle.gps_transport_factory_decorator'));

        $kernel->shutdown();
    }
}
