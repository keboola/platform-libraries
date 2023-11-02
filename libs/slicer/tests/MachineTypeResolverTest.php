<?php

declare(strict_types=1);

namespace Keboola\Slicer\Tests;

use Generator;
use Keboola\Slicer\MachineTypeResolver;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class MachineTypeResolverTest extends TestCase
{
    /** @dataProvider platformNameProvider */
    public function testGetPlatformName(
        string $machineType,
        string $operatingSystem,
        string $expectedPlatformName,
    ): void {
        $machineTypeResolver = new MachineTypeResolver($machineType, $operatingSystem);
        self::assertSame($expectedPlatformName, $machineTypeResolver->getPlatformName());
    }

    public function platformNameProvider(): Generator
    {
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'Linux',
            'expectedPlatformName' => 'amd64',
        ];
        yield [
            'machineType' => 'AMD64',
            'operatingSystem' => 'windows',
            'expectedPlatformName' => 'amd64',
        ];
        yield [
            'machineType' => 'ARM64',
            'operatingSystem' => 'windows',
            'expectedPlatformName' => 'arm64',
        ];
    }

    /** @dataProvider invalidPlatformNameProvider */
    public function testGetPlatformNameError(
        string $machineType,
        string $operatingSystem,
    ): void {
        $machineTypeResolver = new MachineTypeResolver($machineType, $operatingSystem);
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(sprintf('Unsupported platform "%s".', strtoupper($machineType)));
        $machineTypeResolver->getPlatformName();
    }

    public function invalidPlatformNameProvider(): Generator
    {
        yield [
            'machineType' => 'i686',
            'operatingSystem' => 'Linux',
        ];
        yield [
            'machineType' => 'i386',
            'operatingSystem' => 'windows',
        ];
    }

    /** @dataProvider operatingSystemProvider */
    public function testGetOperatingSystemName(
        string $machineType,
        string $operatingSystem,
        string $expectedOperatingSystemName,
    ): void {
        $machineTypeResolver = new MachineTypeResolver($machineType, $operatingSystem);
        self::assertSame($expectedOperatingSystemName, $machineTypeResolver->getOperatingSystemName());
    }

    public function operatingSystemProvider(): Generator
    {
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'Linux',
            'expectedOperatingSystemName' => 'linux',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'Darwin',
            'expectedOperatingSystemName' => 'macos',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'FreeBSD',
            'expectedOperatingSystemName' => 'linux',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'NetBSD',
            'expectedOperatingSystemName' => 'linux',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'OpenBSD',
            'expectedOperatingSystemName' => 'linux',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'SunOS',
            'expectedOperatingSystemName' => 'linux',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'Unix',
            'expectedOperatingSystemName' => 'linux',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'WIN32',
            'expectedOperatingSystemName' => 'win',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'WINNT',
            'expectedOperatingSystemName' => 'win',
        ];
        yield [
            'machineType' => 'x86_64',
            'operatingSystem' => 'Windows',
            'expectedOperatingSystemName' => 'win',
        ];
    }
}
