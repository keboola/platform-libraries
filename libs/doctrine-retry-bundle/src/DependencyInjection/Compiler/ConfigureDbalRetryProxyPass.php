<?php

declare(strict_types=1);

namespace Keboola\DoctrineRetryBundle\DependencyInjection\Compiler;

use Keboola\DoctrineRetryBundle\Database\Retry\Middleware;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

class ConfigureDbalRetryProxyPass implements CompilerPassInterface
{
    private const CONFIG_OPTION_RETRIES = 'x_connect_retries';
    private const SET_MIDDLEWARES_METHOD = 'setMiddlewares';

    public function process(ContainerBuilder $container): void
    {
        $connections = (array) $container->getParameter('doctrine.connections');
        foreach ($connections as $serviceName) {
            $connectionParams = $this->getConnectionParams($container, $serviceName);
            $retries = (int) ($connectionParams['driverOptions'][self::CONFIG_OPTION_RETRIES] ?? 0);

            if ($retries === 0) {
                continue;
            }

            $retryMiddlewareReference = $this->createRetryMiddleware($container, $serviceName, $retries);
            $configurationDefinition = $this->getConfigurationServiceDefinition($container, $serviceName);

            $middlewares = $this->getConfiguredMiddlewares($configurationDefinition);
            $middlewares = [$retryMiddlewareReference, ...$middlewares];

            $configurationDefinition->removeMethodCall(self::SET_MIDDLEWARES_METHOD);
            $configurationDefinition->addMethodCall(self::SET_MIDDLEWARES_METHOD, [$middlewares]);
        }
    }

    private function getConnectionParams(ContainerBuilder $container, string $connectionServiceName): array
    {
        $definition = $container->getDefinition($connectionServiceName);
        $params = $definition->getArgument(0);
        assert(is_array($params));

        return $params;
    }

    private function getConfigurationServiceDefinition(
        ContainerBuilder $container,
        string $connectionServiceName,
    ): Definition {
        $connectionDefinition = $container->getDefinition($connectionServiceName);

        $configurationReference = $connectionDefinition->getArgument(1);
        assert($configurationReference instanceof Reference);

        return $container->getDefinition((string) $configurationReference);
    }

    private function getConfiguredMiddlewares(Definition $configurationDefinition): array
    {
        foreach (array_reverse($configurationDefinition->getMethodCalls()) as [$method, $arguments]) {
            if ($method !== self::SET_MIDDLEWARES_METHOD) {
                continue;
            }

            return $arguments[0];
        }

        return [];
    }

    private function createRetryMiddleware(
        ContainerBuilder $containerBuilder,
        string $namePrefix,
        int $retries,
    ): Reference {
        $retryPolicyServiceName = sprintf('%s.retry_middleware.retry_proxy.retry_policy', $namePrefix);
        $containerBuilder->setDefinition($retryPolicyServiceName, new Definition(SimpleRetryPolicy::class, [
            $retries,
        ]));

        $retryProxyServiceName = sprintf('%s.retry_middleware.retry_proxy', $namePrefix);
        $containerBuilder->setDefinition($retryProxyServiceName, new Definition(RetryProxy::class, [
            new Reference($retryPolicyServiceName),
        ]));

        $middlewareServiceName = sprintf('%s.retry_middleware', $namePrefix);
        $containerBuilder->setDefinition($middlewareServiceName, new Definition(Middleware::class, [
            new Reference($retryProxyServiceName),
        ]));

        return new Reference($middlewareServiceName);
    }
}
