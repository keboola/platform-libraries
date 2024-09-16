<?php

declare(strict_types=1);

namespace Keboola\ServiceClient\Tests;

use Keboola\ServiceClient\Service;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ServiceTest extends TestCase
{
    public function providePublicSubdomains(): iterable
    {
        yield 'ai' => [Service::AI, 'ai'];
        yield 'billing' => [Service::BILLING, 'billing'];
        yield 'buffer' => [Service::BUFFER, 'buffer'];
        yield 'connection' => [Service::CONNECTION, 'connection'];
        yield 'data-science' => [Service::SANDBOXES_SERVICE, 'data-science'];
        yield 'encryption' => [Service::ENCRYPTION, 'encryption'];
        yield 'import' => [Service::IMPORT, 'import'];
        yield 'notification' => [Service::NOTIFICATION, 'notification'];
        yield 'oauth' => [Service::OAUTH, 'oauth'];
        yield 'queue' => [Service::QUEUE, 'queue'];
        yield 'sandboxes' => [Service::SANDBOXES_API, 'sandboxes'];
        yield 'scheduler' => [Service::SCHEDULER, 'scheduler'];
        yield 'sync-actions' => [Service::SYNC_ACTIONS, 'sync-actions'];
    }

    /** @dataProvider providePublicSubdomains */
    public function testGetPublicSubdomain(Service $service, string $expectedValue): void
    {
        self::assertSame($expectedValue, $service->getPublicSubdomain());
    }

    public function provideServicesWithoutPublicDns(): iterable
    {
        yield 'queue internal api' => [Service::QUEUE_INTERNAL_API, 'Job queue internal API does not have public DNS'];
    }

    /** @dataProvider provideServicesWithoutPublicDns */
    public function testGetPublicSubdomainThrowsExceptionForServiceWithoutPublicDns(
        Service $service,
        string $expectedError,
    ): void {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage($expectedError);

        $service->getPublicSubdomain();
    }

    public function provideInternalServiceNames(): iterable
    {
        yield 'ai' => [Service::AI, 'ai-service-api.default'];
        yield 'billing' => [Service::BILLING, 'billing-api.default'];
        yield 'buffer' => [Service::BUFFER, 'buffer-api.buffer']; // <-- custom namespace
        yield 'connection' => [Service::CONNECTION, 'connection-api.connection']; // <-- custom namespace
        yield 'data-science' => [Service::SANDBOXES_SERVICE, 'sandboxes-service-api.default'];
        yield 'encryption' => [Service::ENCRYPTION, 'encryption-api.default'];
        yield 'import' => [Service::IMPORT, 'sapi-importer.default'];
        yield 'notification' => [Service::NOTIFICATION, 'notification-api.default'];
        yield 'oauth' => [Service::OAUTH, 'oauth-api.default'];
        yield 'queue' => [Service::QUEUE, 'job-queue-api.default'];
        yield 'queue internal api' => [Service::QUEUE_INTERNAL_API, 'job-queue-internal-api.default'];
        yield 'sandboxes' => [Service::SANDBOXES_API, 'sandboxes-api.sandboxes']; // <-- custom namespace
        yield 'scheduler' => [Service::SCHEDULER, 'scheduler-api.default'];
        yield 'sync-actions' => [Service::SYNC_ACTIONS, 'runner-sync-api.default'];
    }

    /** @dataProvider provideInternalServiceNames */
    public function testGetInternalServiceName(Service $service, string $expectedValue): void
    {
        self::assertSame($expectedValue, $service->getInternalServiceName());
    }
}
