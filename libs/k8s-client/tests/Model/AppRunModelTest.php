<?php

declare(strict_types=1);

namespace Keboola\K8sClient\Tests\Model;

use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppReference;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRunSpec;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRunStatus;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\PodReference;
use PHPUnit\Framework\TestCase;
use TypeError;

class AppRunModelTest extends TestCase
{
    private static function getAppRunTestData(): array
    {
        return [
            'apiVersion' => 'apps.keboola.com/v1',
            'kind' => 'AppRun',
            'metadata' => [
                'name' => 'apprun-12345',
                'labels' => [
                    'app.kubernetes.io/name' => 'keboola-operator',
                    'app.kubernetes.io/managed-by' => 'kustomize',
                ],
            ],
            'spec' => [
                'podRef' => [
                    'name' => 'app-12345-deployment-abc123-xyz',
                    'uid' => '550e8400-e29b-41d4-a716-446655440000',
                ],
                'appRef' => [
                    'name' => 'app-12345',
                    'appId' => 'app-123',
                    'projectId' => 'project-456',
                ],
                'createdAt' => '2025-01-15T12:00:00Z',
                'startedAt' => '2025-01-15T12:01:00Z',
                'stoppedAt' => '2025-01-15T13:00:00Z',
                'state' => 'Finished',
                'startupLogs' => "foo\nbar\n",
            ],
            'status' => [
                'syncedAt' => '2025-01-15T13:05:00Z',
                'conditions' => [
                    [
                        'type' => 'Ready',
                        'status' => 'True',
                        'lastTransitionTime' => '2025-01-15T12:01:00Z',
                        'reason' => 'PodRunning',
                        'message' => 'Pod is running',
                    ],
                ],
            ],
        ];
    }

    public function testAppRunModelHydration(): void
    {
        $data = self::getAppRunTestData();
        $appRun = new AppRun($data);

        // Basic metadata
        self::assertNotNull($appRun->metadata);
        self::assertSame('apprun-12345', $appRun->metadata->name);

        // Spec basics
        self::assertNotNull($appRun->spec);
        self::assertInstanceOf(AppRunSpec::class, $appRun->spec);
        // Time fields with isRawObject=true are stored as strings, not Time objects
        // @phpstan-ignore-next-line
        self::assertSame('2025-01-15T12:00:00Z', $appRun->spec->createdAt);
        // @phpstan-ignore-next-line
        self::assertSame('2025-01-15T12:01:00Z', $appRun->spec->startedAt);
        // @phpstan-ignore-next-line
        self::assertSame('2025-01-15T13:00:00Z', $appRun->spec->stoppedAt);
        self::assertSame('Finished', $appRun->spec->state);
        self::assertSame("foo\nbar\n", $appRun->spec->startupLogs);

        // PodRef
        self::assertNotNull($appRun->spec->podRef);
        self::assertInstanceOf(PodReference::class, $appRun->spec->podRef);
        self::assertSame('app-12345-deployment-abc123-xyz', $appRun->spec->podRef->name);
        self::assertSame('550e8400-e29b-41d4-a716-446655440000', $appRun->spec->podRef->uid);

        // AppRef
        self::assertNotNull($appRun->spec->appRef);
        self::assertInstanceOf(AppReference::class, $appRun->spec->appRef);
        self::assertSame('app-12345', $appRun->spec->appRef->name);
        self::assertSame('app-123', $appRun->spec->appRef->appId);
        self::assertSame('project-456', $appRun->spec->appRef->projectId);

        // Status
        self::assertNotNull($appRun->status);
        self::assertInstanceOf(AppRunStatus::class, $appRun->status);
        // Time fields with isRawObject=true are stored as strings, not Time objects
        // @phpstan-ignore-next-line
        self::assertSame('2025-01-15T13:05:00Z', $appRun->status->syncedAt);
        self::assertNotNull($appRun->status->conditions);
        self::assertCount(1, $appRun->status->conditions);
        self::assertSame('Ready', $appRun->status->conditions[0]->type);
        self::assertSame('True', $appRun->status->conditions[0]->status);
    }

    public function testAppRunModelSerialization(): void
    {
        $data = self::getAppRunTestData();
        $appRun = new AppRun($data);

        $serialized = $appRun->getArrayCopy();

        self::assertIsArray($serialized);
        self::assertArrayHasKey('metadata', $serialized);
        self::assertArrayHasKey('spec', $serialized);

        // Verify key nested values survive round-trip
        self::assertSame('app-12345-deployment-abc123-xyz', $serialized['spec']['podRef']['name']);
        self::assertSame('app-123', $serialized['spec']['appRef']['appId']);
        self::assertSame('Finished', $serialized['spec']['state']);
    }

    public static function provideInvalidTestData(): iterable
    {
        yield 'wrong state type - array instead of string' => [
            'data' => ['spec' => ['state' => []]],
            'expectedMessage' => 'Cannot assign array to property',
        ];

        yield 'wrong podRef.name type - array instead of string' => [
            'data' => ['spec' => ['podRef' => ['name' => []]]],
            'expectedMessage' => 'Cannot assign array to property',
        ];

        yield 'wrong appRef.appId type - array instead of string' => [
            'data' => ['spec' => ['appRef' => ['appId' => []]]],
            'expectedMessage' => 'Cannot assign array to property',
        ];
    }

    /**
     * @dataProvider provideInvalidTestData
     */
    public function testInvalidDataTypesThrowTypeError(array $data, string $expectedMessage): void
    {
        $this->expectException(TypeError::class);
        $this->expectExceptionMessageMatches('/' . preg_quote($expectedMessage, '/') . '/');

        new AppRun($data);
    }
}
