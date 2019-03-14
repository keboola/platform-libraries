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
     * @var \DateTime
     */
    private $lastImportDate;

    public function __construct(array $configuration)
    {
        $this->source = $configuration['source'];
        try {
            $this->lastImportDate = new \DateTime($configuration['lastImportDate']);
        } catch (\Exception $e) {
            $message = 'Error parsing date "' . $configuration['lastImportDate'] . '": ' . $e->getMessage();
            throw new InvalidDateException($message, null, $e);
        }
    }

    /**
     * @return string
     */
    public function getSource()
    {
        return $this->source;
    }

    /**
     * @return \DateTime
     */
    public function getLastImportDate()
    {
        return $this->lastImportDate;
    }

    public function toArray()
    {
        return [
            'source' => $this->getSource(),
            'lastImportDate' => $this->getLastImportDate()->format(DATE_ISO8601)
        ];
    }
}
