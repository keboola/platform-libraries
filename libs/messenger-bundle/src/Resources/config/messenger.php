<?php

declare(strict_types=1);

use Keboola\MessengerBundle\ConnectionEvent\ApplicationEvent\ApplicationEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\AwsSqsSerializer;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\AzureServiceBusSerializer;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\GooglePubSubSerializer;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use function Symfony\Component\DependencyInjection\Loader\Configurator\abstract_arg;

return static function (ContainerConfigurator $container): void {
    $container->services()
        ->set('keboola.messenger_bundle.event_factory.audit_log', AuditEventFactory::class)
        ->set('keboola.messenger_bundle.event_factory.application_events', ApplicationEventFactory::class)

        ->set('keboola.messenger_bundle.platform_serializer.aws', AwsSqsSerializer::class)
            ->abstract()
            ->arg('$eventFactory', abstract_arg('configured in bundle extension'))

        ->set('keboola.messenger_bundle.platform_serializer.azure', AzureServiceBusSerializer::class)
            ->abstract()
            ->arg('$eventFactory', abstract_arg('configured in bundle extension'))

        ->set('keboola.messenger_bundle.platform_serializer.gcp', GooglePubSubSerializer::class)
            ->abstract()
            ->arg('$eventFactory', abstract_arg('configured in bundle extension'))
    ;
};
