<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Legacy;

class PersistentStorage
{
    private ?bool $ready = null;
    private ?string $k8sStorageClassName = '';

    public static function create(): self
    {
        return new self();
    }

    public static function fromArray(array $values): self
    {
        return self::create()
            ->setReady($values['ready'] ?? null)
            ->setK8sStorageClassName(
                array_key_exists('k8sStorageClassName', $values) ? $values['k8sStorageClassName'] : '',
            )
        ;
    }

    public function setReady(?bool $value): self
    {
        $this->ready = $value;
        return $this;
    }

    public function setK8sStorageClassName(?string $name): self
    {
        $this->k8sStorageClassName = $name;
        return $this;
    }

    public function toArray(): array
    {
        $data = [
            'ready' => $this->ready,
        ];

        if ($this->k8sStorageClassName !== '') {
            $data['k8sStorageClassName'] = $this->k8sStorageClassName;
        }

        return $data;
    }

    public function isReady(): ?bool
    {
        return $this->ready;
    }

    public function getK8sStorageClassName(): ?string
    {
        return $this->k8sStorageClassName;
    }
}
