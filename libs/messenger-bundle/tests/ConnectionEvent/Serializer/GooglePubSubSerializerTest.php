<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\Serializer;

use Keboola\MessengerBundle\ConnectionEvent\EventFactory;
use Keboola\MessengerBundle\ConnectionEvent\GenericEvent;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\GooglePubSubSerializer;
use PHPUnit\Framework\TestCase;
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
        $serializer = new GooglePubSubSerializer(new EventFactory());

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessage($expectedError);

        $serializer->decode($encodedEnvelope);
    }

    public function testValidMessageDecode(): void
    {
        $eventFactory = new class extends EventFactory {
            public function createEventFromArray(array $data): GenericEvent
            {
                return new GenericEvent($data);
            }
        };

        $serializer = new GooglePubSubSerializer($eventFactory);

        $eventData = [
            'data' => [
                'id' => 1234,
                'operation' => 'keboola.test',
            ],
        ];

        $envelope = $serializer->decode($eventData);

        $stamps = $envelope->all();
        self::assertCount(0, $stamps);

        $event = $envelope->getMessage();
        self::assertInstanceOf(GenericEvent::class, $event);
        self::assertSame($eventData, $event->getData());
    }
}
