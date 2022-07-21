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

    /**
     * @var bool
     */
    private $preserveWorkspace;

    public function __construct($devInputsDisabled, $preserveWorkspace = true)
    {
        $this->devInputsDisabled = $devInputsDisabled;
        $this->preserveWorkspace = $preserveWorkspace;
    }

    public function devInputsDisabled()
    {
        return $this->devInputsDisabled;
    }

    public function preserveWorkspace()
    {
        return $this->preserveWorkspace;
    }
}
