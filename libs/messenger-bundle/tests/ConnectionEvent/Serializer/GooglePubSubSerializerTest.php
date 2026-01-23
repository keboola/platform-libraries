<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\Serializer;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\GenericAuditLogEvent;
use Keboola\MessengerBundle\ConnectionEvent\EventFactoryInterface;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\GooglePubSubSerializer;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;

class GooglePubSubSerializerTest extends TestCase
{
    public static function provideInvalidMessageDecodeTestData(): iterable
    {
        yield 'invalid event data' => [
            'data' => [],
            'error' => 'Failed to create an event object from the message: Missing or invalid property "data"',
        ];
    }

    /** @dataProvider provideInvalidMessageDecodeTestData */
    public function testInvalidMessageDecode(array $encodedEnvelope, string $expectedError): void
    {
        $serializer = new GooglePubSubSerializer(new AuditEventFactory());

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessage($expectedError);

        $serializer->decode($encodedEnvelope); // @phpstan-ignore argument.type
    }

    public function testValidMessageDecode(): void
    {
        $eventFactory = new class extends AuditEventFactory {
            public function createEventFromArray(array $data): GenericAuditLogEvent
            {
                return new GenericAuditLogEvent($data);
            }
        };

        $serializer = new GooglePubSubSerializer($eventFactory);

        $eventData = [
            'data' => [
                'id' => 1234,
                'operation' => 'keboola.test',
            ],
        ];

        $envelope = $serializer->decode($eventData); // @phpstan-ignore argument.type

        $stamps = $envelope->all();
        self::assertCount(0, $stamps);

        $event = $envelope->getMessage();
        self::assertInstanceOf(GenericAuditLogEvent::class, $event);
        self::assertSame($eventData, $event->getData());
    }

    public function testEncodingIsDisabled(): void
    {
        $eventFactory = $this->createMock(EventFactoryInterface::class);
        $serializer = new GooglePubSubSerializer($eventFactory);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\Serializer\GooglePubSubSerializer ' .
            'does not support encoding messages',
        );

        $serializer->encode(new Envelope(new stdClass));
    }
}
