<?php

namespace Keboola\InputMapping\State;

use JsonSerializable;

class InputFileState implements JsonSerializable
{
    /**
     * @var array
     */
    private $tags;

    /**
     * @var string
     */
    private $lastImportId;

    public function __construct(array $configuration)
    {
        $this->tags = $configuration['tags'];
        $this->lastImportId = $configuration['lastImportId'];
    }

    /**
     * @return string
     */
    public function getTags()
    {
        return $this->tags;
    }

    /**
     * @return string
     */
    public function getLastImportId()
    {
        return $this->lastImportId;
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return [
            'tags' => $this->getTags(),
            'lastImportId' => $this->getLastImportId()
        ];
    }
}
