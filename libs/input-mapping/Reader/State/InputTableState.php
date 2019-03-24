<?php

namespace Keboola\InputMapping\Reader\State;

use Keboola\InputMapping\Reader\State\Exception\InvalidDateException;

class InputTableState
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

    public function toArray()
    {
        return [
            'source' => $this->getSource(),
            'lastImportDate' => $this->getLastImportDate()
        ];
    }
}
