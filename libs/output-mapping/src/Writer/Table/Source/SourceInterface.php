<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

interface SourceInterface
{
    /**
     * @return string
     */
    public function getName();

    /**
     * @return bool
     */
    public function isSliced();
}
