<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

class TableSource extends AbstractSource
{
    /** @var string */
    private $table;

    public function __construct($sourcePathPrefix, $table, SplFileInfo $manifestFile = null, array $mapping = null)
    {
        parent::__construct($sourcePathPrefix, $manifestFile, $mapping);

        $this->table = $table;
    }

    public function getSourceName()
    {
        return $this->table;
    }

    public function getSourceId()
    {
        return $this->table;
    }
}
