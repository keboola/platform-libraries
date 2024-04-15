<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Legacy;

class SandboxSizeParameters
{
    private ?int $storageSize_GB = null;

    public static function create(): self
    {
        return new self();
    }

    public static function fromArray(array $data): self
    {
        $instance = new self();
        $instance->setStorageSizeGB($data['storageSize_GB'] ?? null);

        return $instance;
    }

    public function toArray(): array
    {
        $data = [];

        if ($this->storageSize_GB !== null) {
            $data['storageSize_GB'] = $this->storageSize_GB;
        }

        return $data;
    }

    public function getStorageSizeGB(): ?int
    {
        return $this->storageSize_GB;
    }

    public function setStorageSizeGB(?int $storageSize_GB): self
    {
        $this->storageSize_GB = $storageSize_GB;
        return $this;
    }
}
