<?php

declare(strict_types=1);

namespace Keboola\InputMapping\State;

use JsonSerializable;

class InputFileState implements JsonSerializable
{
    private array $tags;
    private string $lastImportId;

    public function __construct(array $configuration)
    {
        $this->tags = (array) $configuration['tags'];
        $this->lastImportId = (string) $configuration['lastImportId'];
    }

    public function getTags(): array
    {
        return $this->tags;
    }

    public function getLastImportId(): string
    {
        return $this->lastImportId;
    }

    public function jsonSerialize(): array
    {
        return [
            'tags' => $this->getTags(),
            'lastImportId' => $this->getLastImportId(),
        ];
    }
}
