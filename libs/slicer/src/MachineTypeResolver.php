<?php

declare(strict_types=1);

namespace Keboola\Slicer;

use RuntimeException;

class MachineTypeResolver
{
    private readonly string $operatingSystem;
    private readonly string $machineType;

    public function __construct(string $machineType, string $operatingSystem)
    {
        $this->machineType = strtoupper($machineType);
        $this->operatingSystem = strtoupper($operatingSystem);
    }

    public function getSuffix(): string
    {
        if (str_contains($this->operatingSystem, 'WIN')) {
            return '.exe';
        }
        return '';
    }

    public function getOperatingSystemName(): string
    {
        if (str_contains($this->operatingSystem, 'DARWIN')) {
            return 'macos';
        } elseif (str_contains($this->operatingSystem, 'WIN')) {
            return 'win';
        } else {
            return 'linux';
        }
    }

    public function getPlatformName(): string
    {
        if (str_contains($this->machineType, 'ARM64')) {
            return 'arm64';
        } elseif (str_contains($this->machineType, '64')) {
            return 'amd64';
        } else {
            throw new RuntimeException(sprintf('Unsupported platform "%s".', $this->machineType));
        }
    }
}
