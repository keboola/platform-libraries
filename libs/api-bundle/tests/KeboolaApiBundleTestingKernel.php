<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests;

use CuyZ\ValinorBundle\ValinorBundle;
use Keboola\ApiBundle\DependencyInjection\KeboolaApiExtension;
use Keboola\ApiBundle\KeboolaApiBundle;
use Keboola\ApiBundle\Security\ApplicationToken\ManageApiClientFactory;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\MonologBundle\MonologBundle;
use Symfony\Component\Config\Loader\LoaderInterface;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Kernel;

class KeboolaApiBundleTestingKernel extends Kernel
{
    public function registerBundles(): iterable
    {
        yield new FrameworkBundle();
        yield new MonologBundle();
        yield new ValinorBundle();
        yield new KeboolaApiBundle();
    }

    protected function build(ContainerBuilder $container): void
    {
        // This minimal kernel wires no security firewall, so the auth services would otherwise be
        // private/removed. Keep the ones AuthenticatorTestTrait fetches/replaces public so tests
        // can initialize ManageApiClientFactory and swap in the fake exchange resolver client.
        $container->addCompilerPass(
            new class implements CompilerPassInterface {
                public function process(ContainerBuilder $container): void
                {
                    $publicIds = [
                        ManageApiClientFactory::class,
                        KeboolaApiExtension::STORAGE_TOKEN_RESOLVER_CLIENT_ID,
                    ];
                    foreach ($publicIds as $id) {
                        if ($container->hasDefinition($id)) {
                            $container->getDefinition($id)->setPublic(true);
                        }
                    }
                }
            },
            PassConfig::TYPE_BEFORE_REMOVING,
        );
    }

    public function registerContainerConfiguration(LoaderInterface $loader): void
    {
        $loader->load(__DIR__.'/config.yaml');
    }
}
