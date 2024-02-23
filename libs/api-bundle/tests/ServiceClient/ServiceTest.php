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
        yield 'oauth' => [Service::OAUTH, 'oauth'];
        yield 'mlflow' => [Service::MLFLOW, 'mlflow'];
        yield 'notification' => [Service::NOTIFICATION, 'notification'];
        yield 'sandboxes' => [Service::SANDBOXES, 'sandboxes'];
        yield 'scheduler' => [Service::SCHEDULER, 'scheduler'];
        yield 'spark' => [Service::SPARK, 'spark'];
        yield 'sync-actions' => [Service::SYNC_ACTIONS, 'sync-actions'];
        yield 'queue' => [Service::QUEUE, 'queue'];
    }

    /** @dataProvider providePublicSubdomains */
    public function testGetPublicSubdomain(Service $service, string $expectedValue): void
    {
        self::assertSame($expectedValue, $service->getPublicSubdomain());
    }

    public function provideInternalServiceNames(): iterable
    {
        yield 'ai' => [Service::AI, 'ai'];
        yield 'billing' => [Service::BILLING, 'billing'];
        yield 'buffer' => [Service::BUFFER, 'buffer'];
        yield 'connection' => [Service::CONNECTION, 'connection'];
        yield 'data-science' => [Service::DATA_SCIENCE, 'sandboxes-service']; // <-- different internal name
        yield 'encryption' => [Service::ENCRYPTION, 'encryption'];
        yield 'import' => [Service::IMPORT, 'import'];
        yield 'oauth' => [Service::OAUTH, 'oauth'];
        yield 'mlflow' => [Service::MLFLOW, 'mlflow'];
        yield 'notification' => [Service::NOTIFICATION, 'notification'];
        yield 'sandboxes' => [Service::SANDBOXES, 'sandboxes'];
        yield 'scheduler' => [Service::SCHEDULER, 'scheduler'];
        yield 'spark' => [Service::SPARK, 'spark'];
        yield 'sync-actions' => [Service::SYNC_ACTIONS, 'sync-actions'];
        yield 'queue' => [Service::QUEUE, 'queue'];
    }

    /** @dataProvider provideInternalServiceNames */
    public function testGetInternalServiceName(Service $service, string $expectedValue): void
    {
        self::assertSame($expectedValue, $service->getInternalServiceName());
    }

    public function provideInternalServiceNamespaces(): iterable
    {
        yield 'ai' => [Service::AI, 'default'];
        yield 'billing' => [Service::BILLING, 'default'];
        yield 'buffer' => [Service::BUFFER, 'buffer']; // <-- custom namespace
        yield 'connection' => [Service::CONNECTION, 'connection']; // <-- custom namespace
        yield 'data-science' => [Service::DATA_SCIENCE, 'default'];
        yield 'encryption' => [Service::ENCRYPTION, 'default'];
        yield 'import' => [Service::IMPORT, 'default'];
        yield 'oauth' => [Service::OAUTH, 'default'];
        yield 'mlflow' => [Service::MLFLOW, 'default'];
        yield 'notification' => [Service::NOTIFICATION, 'default'];
        yield 'sandboxes' => [Service::SANDBOXES, 'sandboxes']; // <-- custom namespace
        yield 'scheduler' => [Service::SCHEDULER, 'default'];
        yield 'spark' => [Service::SPARK, 'default'];
        yield 'sync-actions' => [Service::SYNC_ACTIONS, 'default'];
        yield 'queue' => [Service::QUEUE, 'default'];
    }

    /** @dataProvider provideInternalServiceNamespaces */
    public function testGetInternalServiceNamespace(Service $service, string $expectedValue): void
    {
        self::assertSame($expectedValue, $service->getInternalServiceNamespace());
    }
}
