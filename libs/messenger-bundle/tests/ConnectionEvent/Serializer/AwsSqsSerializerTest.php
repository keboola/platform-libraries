<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\Serializer;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\GenericAuditLogEvent;
use Keboola\MessengerBundle\ConnectionEvent\Serializer\AwsSqsSerializer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;

class AwsSqsSerializerTest extends TestCase
{
    /**
     * @dataProvider provideInvalidMessageDecodeTestData
     */
    public function testInvalidMessageDecode(array $encodedEnvelope, string $expectedError): void
    {
        $serializer = new AwsSqsSerializer(new AuditEventFactory());

        $this->expectException(MessageDecodingFailedException::class);
        $this->expectExceptionMessage($expectedError);

        $serializer->decode($encodedEnvelope);
    }

    public function provideInvalidMessageDecodeTestData(): iterable
    {
        yield 'empty encoded envelope' => [
            'data' => [],
            'error' => 'Message is missing body',
        ];

        yield 'body not JSON' => [
            'data' => ['body' => '[}'],
            'error' => 'Message body is not a valid JSON: State mismatch (invalid or malformed JSON)',
        ];

        yield 'body content not array' => [
            'data' => ['body' => json_encode(false, JSON_THROW_ON_ERROR)],
            'error' => 'Message body must be an array',
        ];

        yield 'body content missing Message' => [
            'data' => ['body' => json_encode([], JSON_THROW_ON_ERROR)],
            'error' => 'Message is missing a "Message" property. Was it passed through SNS?',
        ];

        yield 'body content Message not valid JSON' => [
            'data' => ['body' => json_encode([
                'Message' => '[}',
            ], JSON_THROW_ON_ERROR)],
            'error' => 'Message is not a valid JSON: State mismatch (invalid or malformed JSON)',
        ];

        yield 'body content Message not array' => [
            'data' => ['body' => json_encode([
                'Message' => json_encode(false, JSON_THROW_ON_ERROR),
            ], JSON_THROW_ON_ERROR)],
            'error' => 'Event data must be an array',
        ];

        yield 'invalid event data' => [
            'data' => ['body' => json_encode([
                'Message' => json_encode([], JSON_THROW_ON_ERROR),
            ], JSON_THROW_ON_ERROR)],
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

        $serializer = new AwsSqsSerializer($eventFactory);

        $eventData = [
            'id' => 1234,
            'operation' => 'keboola.test',
        ];

        $envelope = $serializer->decode([
            'body' => json_encode([
                'Message' => json_encode([
                    'data' => $eventData,
                ], JSON_THROW_ON_ERROR),
            ], JSON_THROW_ON_ERROR),
        ]);

        $stamps = $envelope->all();
        self::assertCount(0, $stamps);

        $event = $envelope->getMessage();
        self::assertInstanceOf(GenericAuditLogEvent::class, $event);
        self::assertSame($eventData, $event->getData());
    }
}
