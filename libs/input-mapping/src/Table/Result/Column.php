<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Result;

class Column
{
    private string $name;
    /** @var MetadataItem[] */
    private array $metadata;

    public function __construct(string $name, array $metadata)
    {
        $this->name = $name;
        foreach ($metadata as $metadatum) {
            $this->metadata[] = new MetadataItem($metadatum);
        }
    }

    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return MetadataItem[]
     */
    public function getMetadata(): array
    {
        return $this->metadata;
    }
}
