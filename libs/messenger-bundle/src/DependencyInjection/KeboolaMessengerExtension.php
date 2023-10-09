<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\DependencyInjection;

use InvalidArgumentException;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\PrependExtensionInterface;
use Symfony\Component\HttpKernel\DependencyInjection\Extension;

class KeboolaMessengerExtension extends Extension implements PrependExtensionInterface
{
    public function prepend(ContainerBuilder $container): void
    {
        $config = $this->processConfiguration(
            new Configuration(),
            $container->getExtensionConfig('keboola_messenger'),
        );

        if (!empty($config['connection_events_queue_dsn'])) {
            $transportConfig = match ($config['platform']) {
                Configuration::PLATFORM_AWS => [
                    'dsn' => $config['connection_events_queue_dsn'],
                    'serializer' => 'keboola.messenger_bundle.serializer.aws',
                    'options' => [
                        'auto_setup' => false,
                    ],
                ],

                Configuration::PLATFORM_AZURE => [
                    'dsn' => $config['connection_events_queue_dsn'],
                    'serializer' => 'keboola.messenger_bundle.serializer.azure',
                    'options' => [
                        'auto_setup' => false,
                        'entity_path' => $config['connection_events_queue_name'],
                        'receive_mode' => 'peek-lock',
                    ],
                ],

                default => throw new InvalidArgumentException(sprintf('Unknown platform "%s".', $config['platform'])),
            };

            $messengerConfig = $container->getExtensionConfig('messenger');
            $messengerConfig['transports']['connection_events'] = $transportConfig;

            $container->prependExtensionConfig('messenger', $messengerConfig);
        }
    }

    public function load(array $configs, ContainerBuilder $container): void
    {
    }
}
