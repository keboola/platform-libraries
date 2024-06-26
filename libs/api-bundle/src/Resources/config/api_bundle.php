<?php

declare(strict_types=1);

use Keboola\ApiBundle\RequestMapper\ArgumentResolver;
use Keboola\ApiBundle\RequestMapper\DataMapper;
use Keboola\ApiBundle\Security\AttributeAuthenticator;
use Keboola\ApiBundle\Util\ControllerReflector;
use Keboola\PermissionChecker\PermissionChecker;
use Keboola\ServiceClient\ServiceClient;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\DependencyInjection\ServiceLocator;
use function Symfony\Component\DependencyInjection\Loader\Configurator\env;
use function Symfony\Component\DependencyInjection\Loader\Configurator\param;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $container->services()
        ->set('keboola.api_bundle.security.attribute_authenticator', AttributeAuthenticator::class)
        ->args([
            service('keboola.api_bundle.security.controller_reflector'),
            service('keboola.api_bundle.security.authenticators_locator'),
        ])

        ->set('keboola.api_bundle.security.authenticators_locator', ServiceLocator::class)
        ->tag('container.service_locator')

        ->set('keboola.api_bundle.security.controller_reflector', ControllerReflector::class)
        ->args([
            service('service_container'),
        ])

        ->set(PermissionChecker::class)
        ->alias('keboola.api_bundle.security.permission_checker', PermissionChecker::class)

        ->set(ServiceClient::class)
            ->arg('$hostnameSuffix', env('HOSTNAME_SUFFIX'))
            ->arg('$defaultDnsType', param('keboola_api_bundle.default_service_dns_type'))
        ->alias('keboola.api_bundle.service_client', ServiceClient::class)

        ->set(DataMapper::class)
            ->arg('$mapperBuilder', service('valinor.mapper_builder'))
            ->arg('$validator', service('validator'))
        ->alias('keboola.api_bundle.request_mapper.data_mapper', DataMapper::class)

        ->set(ArgumentResolver::class)
            ->arg('$dataMapper', service(DataMapper::class))
            ->tag('controller.argument_value_resolver')
        ->alias('keboola.api_bundle.request_mapper.argument_resolver', ArgumentResolver::class)
    ;
};
