<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\Serializer;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\GenericAuditLogEvent;
use Keboola\MessengerBundle\ConnectionEvent\EventFactoryInterface;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\AzureServiceBusSerializer;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;

class AzureServiceBusSerializerTest extends TestCase
{
    /**
     * @dataProvider provideInvalidMessageDecodeTestData
     */
    public function testInvalidMessageDecode(array $encodedEnvelope, string $expectedError): void
    {
        $serializer = new AzureServiceBusSerializer(new AuditEventFactory());

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessage($expectedError);

        $serializer->decode($encodedEnvelope); // @phpstan-ignore argument.type
    }

    public function provideInvalidMessageDecodeTestData(): iterable
    {
        yield 'empty encoded envelope' => [
            'data' => [],
            'error' => 'Message body is missing',
        ];

        yield 'body not JSON' => [
            'data' => ['body' => '[}'],
            'error' => 'Message body is not a valid JSON: State mismatch (invalid or malformed JSON)',
        ];

        yield 'body content not array' => [
            'data' => ['body' => json_encode(false, JSON_THROW_ON_ERROR)],
            'error' => 'Message body must be an array',
        ];

        yield 'body content missing data' => [
            'data' => ['body' => json_encode([], JSON_THROW_ON_ERROR)],
            'error' => 'Message is missing a "data" property. Was it passed through EventGrid?',
        ];

        yield 'body content data not array' => [
            'data' => ['body' => json_encode(['data' => false], JSON_THROW_ON_ERROR)],
            'error' => 'Message body data must be an array',
        ];

        yield 'invalid event data' => [
            'data' => ['body' => json_encode(['data' => []], JSON_THROW_ON_ERROR)],
            'error' => 'Failed to create an event object from the message: Missing or invalid property "data"',
        ];
    }

    public function testValidMessageDecode(): void
    {
        $eventFactory = new class extends AuditEventFactory {
            public function createEventFromArray(array $data): GenericAuditLogEvent
            {
                return new GenericAuditLogEvent($data['data']);
            }
        };

        $serializer = new AzureServiceBusSerializer($eventFactory);

        $eventData = [
            'id' => 1234,
            'operation' => 'keboola.test',
        ];

        $envelope = $serializer->decode([
            'body' => json_encode([
                'data' => [
                    'data' => $eventData,
                ],
            ], JSON_THROW_ON_ERROR),
        ]);

        $stamps = $envelope->all();
        self::assertCount(0, $stamps);

        $event = $envelope->getMessage();
        self::assertInstanceOf(GenericAuditLogEvent::class, $event);
        self::assertSame($eventData, $event->getData());
    }

    public function testEncodingIsDisabled(): void
    {
        $eventFactory = $this->createMock(EventFactoryInterface::class);
        $serializer = new AzureServiceBusSerializer($eventFactory);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keboola\MessengerBundle\ConnectionEvent\Serializer\AzureServiceBusSerializer ' .
            'does not support encoding messages',
        );

        $serializer->encode(new Envelope(new stdClass));
    }
}
