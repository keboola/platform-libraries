<?php

namespace Keboola\InputMapping\Table\Result;

use Generator;

class Column
{
    /** @var string */
    private $name;
    /** @var MetadataItem[] */
    private $metadata;

    /**
     * @param string $name
     * @param array $metadata
     */
    public function __construct($name, array $metadata)
    {
        $this->name = $name;
        foreach ($metadata as $metadatum) {
            $this->metadata[] = new MetadataItem($metadatum);
        }
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return Generator
     */
    public function getMetadata()
    {
        foreach ($this->metadata as $metadatum) {
            yield $metadatum;
        }
    }
}
