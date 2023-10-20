<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent;

use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\TransportInterface;

abstract class AbstractQueueConsumptionTest extends KernelTestCase
{
    private const MAX_MESSAGE_WAIT_TIME = 40; // 30 seconds is the default visibility timeout + 10 seconds margin
    protected const EVENT_DATA = [
        'class' => 'Event_AuditLog_Projects_ProjectEvent',
        'created' => '2022-03-10T15:16:09+01:00',
        'data' => [
            'id' => '1234',
            'operation' => 'auditLog.project.deleted',
            'request' => [
                'remoteAddr' => '172.16.0.133:54847',
                'httpReferer' => 'https://connection.east-us-2.azure.keboola-testing.com/admin',
            ],
            'admin' => [
                'id' => 14,
                'name' => ' ',
                'email' => 'tomas.kacur@keboolaconnection.onmicrosoft.com',
            ],
            'auditLogEventCreatedAt' => '2022-03-10T15:16:09+0100',
            'context' => [
                'project' => [
                    'id' => 16,
                    'name' => 'Sandbox Test',
                ],
            ],
        ],
    ];

    protected TransportInterface $messengerTransport;

    protected function setUp(): void
    {
        parent::setUp();

        $this->messengerTransport = self::getContainer()->get('messenger.transport.connection_events');
    }

    abstract protected function publishMessage(array $message): void;

    /**
     * @return Envelope[]
     */
    protected function waitAndFetchAllAvailableMessages(): array
    {
        /** @var Envelope[] $messages */
        $messages = [];

        foreach ($this->messengerTransport->get() as $message) {
            $this->messengerTransport->ack($message);
            $messages[] = $message;
        }

        // wait for some message to come
        $sleepCounter = self::MAX_MESSAGE_WAIT_TIME;
        while (count($messages) === 0) {
            sleep(1);
            foreach ($this->messengerTransport->get() as $message) {
                $this->messengerTransport->ack($message);
                $messages[] = $message;
            }

            if (--$sleepCounter <= 0) {
                $this->fail(sprintf(
                    'No message available from queue after %ssec of sleep',
                    self::MAX_MESSAGE_WAIT_TIME,
                ));
            }
        }

        // fetch all messages available
        do {
            $fetched = false;

            foreach ($this->messengerTransport->get() as $message) {
                $this->messengerTransport->ack($message);
                $messages[] = $message;
                $fetched = true;
            }
        } while ($fetched !== false);

        return $messages;
    }
}
