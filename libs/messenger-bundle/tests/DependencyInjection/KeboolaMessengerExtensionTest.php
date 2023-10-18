<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\DependencyInjection;

use Keboola\MessengerBundle\DependencyInjection\KeboolaMessengerExtension;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Symfony\Component\DependencyInjection\ChildDefinition;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBag;
use Symfony\Component\DependencyInjection\Reference;

class KeboolaMessengerExtensionTest extends KernelTestCase
{
    public static function provideConfigTestData(): iterable
    {
        yield 'bundle disabled' => [
            'bundleConfig' => [
                'platform' => null,
            ],
            'configuredTransports' => [],
        ];

        yield 'bundle enabled' => [
            'bundleConfig' => [
                'platform' => 'test',
            ],
            'configuredTransports' => [
                'async' => [
                    'dsn' => '%env(resolve:KEBOOLA_MESSENGER_TEST_CONNECTION_EVENTS_QUEUE_DSN)%',
                ],
                'audit_log' => [
                    'dsn' => '%env(resolve:KEBOOLA_MESSENGER_TEST_CONNECTION_AUDIT_LOG_QUEUE_DSN)%',
                ],
            ],
        ];
    }

    public function testBundleDisabled(): void
    {
        $containerBuilder = $this->createContainerBuilder([
            'platform' => null,
        ]);

        $extension = new KeboolaMessengerExtension();
        $extension->prepend($containerBuilder);

        $frameworkConfigs = $containerBuilder->getExtensionConfig('framework');
        self::assertCount(0, $frameworkConfigs);
    }

    public static function provideData(): iterable
    {
        yield 'aws with events queue' => [
            'bundleConfig' => [
                'platform' => 'aws',
                'connection_events_queue_dsn' => 'http://example.com',
            ],
            'transports' => [
                'connection_events' => [
                    'dsn' => 'http://example.com',
                    'serializer' => 'keboola.messenger_bundle.transport_serializer.connection_events',
                    'options' => [
                        'auto_setup' => false,
                    ],
                ],
            ],
            'serializers' => [
                [
                    'name' => 'keboola.messenger_bundle.transport_serializer.connection_events',
                    'parent' => 'keboola.messenger_bundle.platform_serializer.aws',
                    'eventFactory' => 'keboola.messenger_bundle.event_factory.application_events',
                ],
            ],
        ];

        yield 'aws with events & audit log queue' => [
            'bundleConfig' => [
                'platform' => 'aws',
                'connection_events_queue_dsn' => 'http://example.com/events',
                'connection_audit_log_queue_dsn' => 'http://example.com/audit-log',
            ],
            'transports' => [
                'connection_audit_log' => [
                    'dsn' => 'http://example.com/audit-log',
                    'serializer' => 'keboola.messenger_bundle.transport_serializer.connection_audit_log',
                    'options' => [
                        'auto_setup' => false,
                    ],
                ],
                'connection_events' => [
                    'dsn' => 'http://example.com/events',
                    'serializer' => 'keboola.messenger_bundle.transport_serializer.connection_events',
                    'options' => [
                        'auto_setup' => false,
                    ],
                ],
            ],
            'serializers' => [
                [
                    'name' => 'keboola.messenger_bundle.transport_serializer.connection_events',
                    'parent' => 'keboola.messenger_bundle.platform_serializer.aws',
                    'eventFactory' => 'keboola.messenger_bundle.event_factory.application_events',
                ],
                [
                    'name' => 'keboola.messenger_bundle.transport_serializer.connection_audit_log',
                    'parent' => 'keboola.messenger_bundle.platform_serializer.aws',
                    'eventFactory' => 'keboola.messenger_bundle.event_factory.audit_log',
                ],
            ],
        ];

        yield 'gcp with audit log queue' => [
            'bundleConfig' => [
                'platform' => 'gcp',
                'connection_audit_log_queue_dsn' => 'http://example.com',
            ],
            'transports' => [
                'connection_audit_log' => [
                    'dsn' => 'http://example.com',
                    'serializer' => 'keboola.messenger_bundle.transport_serializer.connection_audit_log',
                    'options' => [],
                ],
            ],
            'serializers' => [
                [
                    'name' => 'keboola.messenger_bundle.transport_serializer.connection_audit_log',
                    'parent' => 'keboola.messenger_bundle.platform_serializer.gcp',
                    'eventFactory' => 'keboola.messenger_bundle.event_factory.audit_log',
                ],
            ],
        ];

        yield 'azure with audit log queue' => [
            'bundleConfig' => [
                'platform' => 'azure',
                'connection_audit_log_queue_dsn' => 'http://example.com',
            ],
            'transports' => [
                'connection_audit_log' => [
                    'dsn' => 'http://example.com',
                    'serializer' => 'keboola.messenger_bundle.transport_serializer.connection_audit_log',
                    'options' => [
                        'token_expiry' => 3600,
                        'receive_mode' => 'peek-lock',
                    ],
                ],
            ],
            'serializers' => [
                [
                    'name' => 'keboola.messenger_bundle.transport_serializer.connection_audit_log',
                    'parent' => 'keboola.messenger_bundle.platform_serializer.azure',
                    'eventFactory' => 'keboola.messenger_bundle.event_factory.audit_log',
                ],
            ],
        ];
    }

    /** @dataProvider provideData */
    public function testSymfonyMessengerIsConfiguredProperly(
        array $bundleConfig,
        array $expectedTransports,
        array $expectedSerializers,
    ): void {
        $containerBuilder = $this->createContainerBuilder($bundleConfig);

        $extension = new KeboolaMessengerExtension();
        $extension->prepend($containerBuilder);

        // check Symfony messenger transports looks as expected
        $frameworkConfigs = $containerBuilder->getExtensionConfig('framework');
        $frameworkConfig = array_merge_recursive(...$frameworkConfigs);
        self::assertSame($expectedTransports, $frameworkConfig['messenger']['transports'] ?? []);

        // check serializer for each transport exists and has correct event factory set
        foreach ($expectedSerializers as ['name' => $name, 'parent' => $parentName, 'eventFactory' => $eventFactory]) {
            $serializerDefinition = $containerBuilder->getDefinition($name);
            self::assertInstanceOf(ChildDefinition::class, $serializerDefinition);
            self::assertSame($parentName, $serializerDefinition->getParent());

            $eventFactoryArg = $serializerDefinition->getArgument('$eventFactory');
            self::assertInstanceOf(Reference::class, $eventFactoryArg);
            self::assertSame($eventFactory, (string) $eventFactoryArg);
        }
    }

    private function createContainerBuilder(array $bundleConfig): ContainerBuilder
    {
        $containerBuilder = new ContainerBuilder(new ParameterBag([
            'kernel.environment' => 'test',
            'kernel.debug' => true,
            'kernel.build_dir' => '/tmp',
        ]));
        $containerBuilder->prependExtensionConfig('keboola_messenger', $bundleConfig);

        return $containerBuilder;
    }
}
