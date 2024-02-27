<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\ServiceClient;

use Keboola\ApiBundle\ServiceClient\Service;
use PHPUnit\Framework\TestCase;

class ServiceTest extends TestCase
{
    public function providePublicSubdomains(): iterable
    {
        yield 'ai' => [Service::AI, 'ai'];
        yield 'billing' => [Service::BILLING, 'billing'];
        yield 'buffer' => [Service::BUFFER, 'buffer'];
        yield 'connection' => [Service::CONNECTION, 'connection'];
        yield 'data-science' => [Service::DATA_SCIENCE, 'data-science'];
        yield 'encryption' => [Service::ENCRYPTION, 'encryption'];
        yield 'import' => [Service::IMPORT, 'import'];
        yield 'notification' => [Service::NOTIFICATION, 'notification'];
        yield 'oauth' => [Service::OAUTH, 'oauth'];
        yield 'queue' => [Service::QUEUE, 'queue'];
        yield 'sandboxes' => [Service::SANDBOXES, 'sandboxes'];
        yield 'scheduler' => [Service::SCHEDULER, 'scheduler'];
        yield 'sync-actions' => [Service::SYNC_ACTIONS, 'sync-actions'];
    }

    /** @dataProvider providePublicSubdomains */
    public function testGetPublicSubdomain(Service $service, string $expectedValue): void
    {
        self::assertSame($expectedValue, $service->getPublicSubdomain());
    }

    public function provideInternalServiceNames(): iterable
    {
        yield 'ai' => [Service::AI, 'ai-service-api.default'];
        yield 'billing' => [Service::BILLING, 'billing-api.buffer']; // <-- custom namespace
        yield 'buffer' => [Service::BUFFER, 'buffer-api.default'];
        yield 'connection' => [Service::CONNECTION, 'connection-api.connection']; // <-- custom namespace
        yield 'data-science' => [Service::DATA_SCIENCE, 'sandboxes-service-api.default'];
        yield 'encryption' => [Service::ENCRYPTION, 'encryption-api.default'];
        yield 'import' => [Service::IMPORT, 'sapi-importer.default'];
        yield 'notification' => [Service::NOTIFICATION, 'notification-api.default'];
        yield 'oauth' => [Service::OAUTH, 'oauth-api.default'];
        yield 'queue' => [Service::QUEUE, 'job-queue-api.default'];
        yield 'sandboxes' => [Service::SANDBOXES, 'sandboxes-api.sandboxes']; // <-- custom namespace
        yield 'scheduler' => [Service::SCHEDULER, 'scheduler-api.default'];
        yield 'sync-actions' => [Service::SYNC_ACTIONS, 'runner-sync-api.default'];
    }

    /** @dataProvider provideInternalServiceNames */
    public function testGetInternalServiceName(Service $service, string $expectedValue): void
    {
        self::assertSame($expectedValue, $service->getInternalServiceName());
    }
}
