<?php

namespace Keboola\InputMapping\Table\Options;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\TableNotFoundException;
use Keboola\InputMapping\State\InputTableStateList;

class ReaderOptions
{
    /**
     * @var bool
     */
    private $devInputsDisabled;

    public function __construct($devInputsDisabled)
    {
        $this->devInputsDisabled = $devInputsDisabled;
    }

    public function devInputsDisabled()
    {
        return $this->devInputsDisabled;
    }
}
