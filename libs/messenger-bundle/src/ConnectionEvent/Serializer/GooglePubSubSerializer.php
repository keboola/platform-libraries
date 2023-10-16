<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent\Serializer;

use JsonException;
use Keboola\MessengerBundle\ConnectionEvent\EventFactory;
use Keboola\MessengerBundle\ConnectionEvent\Exception\EventFactoryException;
use Keboola\MessengerBundle\ConnectionEvent\GenericEvent;
use RuntimeException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class GooglePubSubSerializer implements SerializerInterface
{
    private EventFactory $eventFactory;

    public function __construct(EventFactory $eventFactory)
    {
        $this->eventFactory = $eventFactory;
    }

    public function decode(array $encodedEnvelope): Envelope
    {
        try {
            $event = $this->eventFactory->createEventFromArray($encodedEnvelope);
        } catch (EventFactoryException $e) {
            throw new MessageDecodingFailedException(
                sprintf('Failed to create an event object from the message: %s', $e->getMessage()),
                0,
                $e,
            );
        }

        return new Envelope($event, []);
    }

    public function encode(Envelope $envelope): array
    {
        throw new RuntimeException(sprintf('%s does not support encoding messages', static::class));
    }
}
