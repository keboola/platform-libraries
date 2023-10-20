<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\AuditLog;

use Keboola\MessengerBundle\ConnectionEvent\AuditLog\AuditEventFactory;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\GenericAuditLogEvent;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectDeletedEvent;
use Keboola\MessengerBundle\ConnectionEvent\Exception\EventFactoryException;
use PHPUnit\Framework\TestCase;

class AuditEventFactoryTest extends TestCase
{
    public static function provideInvalidData(): iterable
    {
        yield 'no "data"' => [
            'data' => [],
            'error' => 'Missing or invalid property "data"',
        ];

        yield 'no "data.operation"' => [
            'data' => [
                'data' => [],
            ],
            'error' => 'Missing property "data.operation"',
        ];
    }

    /** @dataProvider provideInvalidData */
    public function testInvalidData(array $data, string $expectedError): void
    {
        $factory = new AuditEventFactory();

        $this->expectException(EventFactoryException::class);
        $this->expectExceptionMessage($expectedError);

        $factory->createEventFromArray($data);
    }

    public function testErrorInDataMapping(): void
    {
        $factory = new AuditEventFactory();

        $this->expectException(EventFactoryException::class);
        $this->expectExceptionMessage(
            'Failed to create Keboola\MessengerBundle\ConnectionEvent\AuditLog\Organization\ProjectCreatedEvent ' .
            'from event data: Warning: Undefined array key "id"',
        );

        $factory->createEventFromArray([
            'data' => [
                'operation' => 'auditLog.organization.projectCreated',
            ],
        ]);
    }

    public function testCreateRegularEvent(): void
    {
        $factory = new AuditEventFactory();

        $result = $factory->createEventFromArray([
            'data' => [
                'operation' => ProjectDeletedEvent::NAME,
                'id' => '123',
                'auditLogEventCreatedAt' => '2021-01-01T00:00:00+00:00',
                'admin' => [
                    'id' => '456',
                    'name' => 'John Doe',
                    'email' => 'john.doe@example.com',
                ],
                'context' => [
                    'project' => [
                        'id' => '123',
                        'name' => 'foo',
                    ],
                ],
            ],
        ]);

        self::assertInstanceOf(ProjectDeletedEvent::class, $result);
    }

    public function testCreateGenericEvent(): void
    {
        $factory = new AuditEventFactory();

        $result = $factory->createEventFromArray([
            'data' => [
                'operation' => 'unknown.event',
                'id' => '123',
            ],
        ]);

        self::assertInstanceOf(GenericAuditLogEvent::class, $result);
    }
}
