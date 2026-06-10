<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\Transport;

use Keboola\MessengerBundle\Transport\GpsTransportFactoryDecorator;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class GpsTransportFactoryDecoratorTest extends TestCase
{
    public function testSupportsDelegatesToInnerFactory(): void
    {
        $inner = $this->createMock(TransportFactoryInterface::class);
        $inner->expects(self::once())
            ->method('supports')
            ->with('gps://default/topic', ['foo' => 'bar'])
            ->willReturn(true);

        $decorator = new GpsTransportFactoryDecorator($inner);

        self::assertTrue($decorator->supports('gps://default/topic', ['foo' => 'bar']));
    }

    public function testCreateTransportInjectsDefaultClientConfigWhenNotConfigured(): void
    {
        $serializer = $this->createMock(SerializerInterface::class);
        $transport = $this->createMock(TransportInterface::class);

        $inner = $this->createMock(TransportFactoryInterface::class);
        $inner->expects(self::once())
            ->method('createTransport')
            ->with(
                'gps://default/topic',
                [
                    'transport_name' => 'messages',
                    'client_config' => [
                        'restOptions' => [
                            'timeout' => 120,
                            'connect_timeout' => 10,
                        ],
                    ],
                ],
                $serializer,
            )
            ->willReturn($transport);

        $decorator = new GpsTransportFactoryDecorator($inner);

        self::assertSame(
            $transport,
            $decorator->createTransport('gps://default/topic', ['transport_name' => 'messages'], $serializer),
        );
    }

    public function testCreateTransportKeepsClientConfigFromOptions(): void
    {
        $serializer = $this->createMock(SerializerInterface::class);
        $transport = $this->createMock(TransportInterface::class);

        $options = [
            'client_config' => [
                'requestTimeout' => 60,
            ],
        ];

        $inner = $this->createMock(TransportFactoryInterface::class);
        $inner->expects(self::once())
            ->method('createTransport')
            ->with('gps://default/topic', $options, $serializer)
            ->willReturn($transport);

        $decorator = new GpsTransportFactoryDecorator($inner);

        self::assertSame(
            $transport,
            $decorator->createTransport('gps://default/topic', $options, $serializer),
        );
    }

    public function testCreateTransportKeepsClientConfigFromDsn(): void
    {
        $serializer = $this->createMock(SerializerInterface::class);
        $transport = $this->createMock(TransportInterface::class);

        $dsn = 'gps://default/topic?client_config[restOptions][timeout]=30';

        $inner = $this->createMock(TransportFactoryInterface::class);
        $inner->expects(self::once())
            ->method('createTransport')
            ->with($dsn, [], $serializer)
            ->willReturn($transport);

        $decorator = new GpsTransportFactoryDecorator($inner);

        self::assertSame(
            $transport,
            $decorator->createTransport($dsn, [], $serializer),
        );
    }
}
