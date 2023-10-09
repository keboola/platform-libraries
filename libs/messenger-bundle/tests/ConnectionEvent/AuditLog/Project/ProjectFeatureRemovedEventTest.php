<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests\ConnectionEvent\AuditLog\Project;

use DateTimeImmutable;
use Keboola\MessengerBundle\ConnectionEvent\AuditLog\Project\ProjectFeatureRemovedEvent;
use PHPUnit\Framework\TestCase;

class ProjectFeatureRemovedEventTest extends TestCase
{
    public function testCreateAndExport(): void
    {
        $event = ProjectFeatureRemovedEvent::fromArray([
            'id' => 1234,
            'operation' => 'auditLog.project.featureRemoved',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00Z',
            'admin' => [
                'id' => 456,
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
                'project' => [
                    'id' => 4321,
                    'name' => 'project-name',
                ],

                'feature' => [
                    'id' => 963,
                    'name' => 'feature-name',
                    'type' => 'type',
                ],
            ],

            'garbage' => 'ignored',
        ]);

        self::assertSame('1234', $event->getId());
        self::assertEquals(new DateTimeImmutable('2022-03-03T12:00:00Z'), $event->getCreatedAt());

        self::assertSame('456', $event->getAdminId());
        self::assertSame('admin-name', $event->getAdminName());
        self::assertSame('admin@example.com', $event->getAdminEmail());

        self::assertSame('4321', $event->getProjectId());
        self::assertSame('project-name', $event->getProjectName());

        self::assertSame('963', $event->getFeatureId());
        self::assertSame('feature-name', $event->getFeatureName());
        self::assertSame('type', $event->getFeatureType());

        self::assertSame([
            'id' => '1234',
            'operation' => 'auditLog.project.featureRemoved',
            'auditLogEventCreatedAt' => '2022-03-03T12:00:00+0000',
            'admin' => [
                'id' => '456',
                'name' => 'admin-name',
                'email' => 'admin@example.com',
            ],

            'context' => [
                'project' => [
                    'id' => '4321',
                    'name' => 'project-name',
                ],

                'feature' => [
                    'id' => '963',
                    'name' => 'feature-name',
                    'type' => 'type',
                ],
            ],
        ], $event->toArray());
    }
}
