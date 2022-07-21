<?php

namespace Keboola\InputMapping\State;

use JsonSerializable;

class InputTableState implements JsonSerializable
{
    /**
     * @var string
     */
    private $source;

    /**
     * @var string
     */
    private $lastImportDate;

    public function __construct(array $configuration)
    {
        $this->source = $configuration['source'];
        $this->lastImportDate = $configuration['lastImportDate'];
    }

    /**
     * @return string
     */
    public function getSource()
    {
        return $this->source;
    }

    /**
     * @return string
     */
    public function getLastImportDate()
    {
        return $this->lastImportDate;
    }

    /**
     * @return array
     */
    public function jsonSerialize()
    {
        return [
            'source' => $this->getSource(),
            'lastImportDate' => $this->getLastImportDate()
        ];
    }
}
