<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Tests\Sandboxes\Legacy;

use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\SandboxSizeParameters;
use PHPUnit\Framework\TestCase;

class SandboxSizeParametersTest extends TestCase
{
    public function testCreate(): void
    {
        $parameters = SandboxSizeParameters::create();

        self::assertNull($parameters->getStorageSizeGB());
    }

    public function testSetStorageSize(): void
    {
        $parameters = SandboxSizeParameters::create();
        self::assertNull($parameters->getStorageSizeGB());

        $parameters->setStorageSizeGB(256);
        self::assertSame(256, $parameters->getStorageSizeGB());

        $parameters->setStorageSizeGB(null);
        self::assertNull($parameters->getStorageSizeGB());
    }

    /**
     * @dataProvider provideCreateFromArrayTestData
     */
    public function testCreateFromArray(array $data, SandboxSizeParameters $expectedParameters): void
    {
        $parameters = SandboxSizeParameters::fromArray($data);

        self::assertEquals($expectedParameters, $parameters);
    }

    public function provideCreateFromArrayTestData(): iterable
    {
        yield 'no parameters' => [
            'data' => [],
            'result' => SandboxSizeParameters::create(),
        ];

        yield 'empty storage size' => [
            'data' => [
                'storageSize_GB' => null,
            ],
            'result' => SandboxSizeParameters::create()->setStorageSizeGB(null),
        ];

        yield 'with storage size' => [
            'data' => [
                'storageSize_GB' => 128,
            ],
            'result' => SandboxSizeParameters::create()->setStorageSizeGB(128),
        ];
    }

    /**
     * @dataProvider provideExportToArrayTestData
     */
    public function testExportToArray(SandboxSizeParameters $parameters, array $expectedData): void
    {
        $data = $parameters->toArray();

        self::assertSame($expectedData, $data);
    }

    public function provideExportToArrayTestData(): iterable
    {
        yield 'empty storage size' => [
            'result' => SandboxSizeParameters::create()->setStorageSizeGB(null),
            'data' => [],
        ];

        yield 'with zero storage size' => [
            'result' => SandboxSizeParameters::create()->setStorageSizeGB(0),
            'data' => [
                'storageSize_GB' => 0,
            ],
        ];

        yield 'with storage size' => [
            'result' => SandboxSizeParameters::create()->setStorageSizeGB(128),
            'data' => [
                'storageSize_GB' => 128,
            ],
        ];
    }
}
