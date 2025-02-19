<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes\Legacy;

class Project
{
    private string $id;

    private ?PersistentStorage $persistentStorage = null;
    private string $createdTimestamp;
    private string $updatedTimestamp;

    public static function fromArray(array $in): self
    {
        $project = new self();
        $project->id = (string) $in['id'];
        $project->createdTimestamp = $in['createdTimestamp'];
        $project->updatedTimestamp = $in['updatedTimestamp'] ?? '';
        $project->persistentStorage = isset($in['persistentStorage'])
            ? PersistentStorage::fromArray($in['persistentStorage'])
            : null;

        return $project;
    }

    public function toArray(): array
    {
        $result = [];
        if (!empty($this->id)) {
            $result['id'] = $this->id;
        }

        if ($this->persistentStorage !== null) {
            $result['persistentStorage'] = $this->persistentStorage->toArray();
        }

        if (!empty($this->createdTimestamp)) {
            $result['createdTimestamp'] = $this->createdTimestamp;
        }
        if (!empty($this->updatedTimestamp)) {
            $result['updatedTimestamp'] = $this->updatedTimestamp;
        }

        return $result;
    }

    public function toApiRequest(): array
    {
        $array = $this->toArray();
        unset($array['id']);
        unset($array['createdTimestamp']);
        unset($array['updatedTimestamp']);
        return $array;
    }

    public function setId(string $id): self
    {
        $this->id = $id;
        return $this;
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function setCreatedTimestamp(string $createdTimestamp): self
    {
        $this->createdTimestamp = $createdTimestamp;
        return $this;
    }

    public function getCreatedTimestamp(): ?string
    {
        return $this->createdTimestamp;
    }

    public function setUpdatedTimestamp(string $updatedTimestamp): self
    {
        $this->updatedTimestamp = $updatedTimestamp;
        return $this;
    }

    public function getUpdatedTimestamp(): ?string
    {
        return $this->updatedTimestamp;
    }

    public function setPersistentStorage(PersistentStorage $persistentStorage): self
    {
        $this->persistentStorage = $persistentStorage;
        return $this;
    }

    public function getPersistentStorage(): ?PersistentStorage
    {
        return $this->persistentStorage;
    }
}
