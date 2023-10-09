<?php

declare(strict_types=1);

use Keboola\MessengerBundle\ConnectionEvent\EventFactory;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\AwsSqsSerializer;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\AzureServiceBusSerializer;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $container->parameters()
        ->set('env(CONNECTION_EVENTS_QUEUE_DSN)', '')
        ->set('env(CONNECTION_EVENTS_QUEUE_NAME)', '')
    ;

    $container->services()
        ->set('keboola.messenger_bundle.connection_event_factory', EventFactory::class)

        ->set('keboola.messenger_bundle.serializer.aws', AwsSqsSerializer::class)
        ->arg('$eventFactory', service('keboola.messenger_bundle.connection_event_factory'))

        ->set('keboola.messenger_bundle.serializer.azure', AzureServiceBusSerializer::class)
        ->arg('$eventFactory', service('keboola.messenger_bundle.connection_event_factory'))
    ;
};
