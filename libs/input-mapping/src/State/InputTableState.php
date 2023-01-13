<?php

declare(strict_types=1);

namespace Keboola\InputMapping\State;

use JsonSerializable;

class InputTableState implements JsonSerializable
{
    private string $source;
    private string $lastImportDate;

    public function __construct(array $configuration)
    {
        $this->source = (string) $configuration['source'];
        $this->lastImportDate = (string) $configuration['lastImportDate'];
    }

    public function getSource(): string
    {
        return $this->source;
    }

    public function getLastImportDate(): string
    {
        return $this->lastImportDate;
    }

    public function jsonSerialize(): array
    {
        return [
            'source' => $this->getSource(),
            'lastImportDate' => $this->getLastImportDate(),
        ];
    }
}
