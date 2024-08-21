<?php

declare(strict_types=1);

namespace Keboola\DoctrineRetryBundle;

use Keboola\DoctrineRetryBundle\DependencyInjection\Compiler\ConfigureDbalRetryProxyPass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;

class KeboolaDoctrineRetryBundle extends Bundle
{
    public function build(ContainerBuilder $container): void
    {
        parent::build($container);

        $container->addCompilerPass(new ConfigureDbalRetryProxyPass());
    }
}
