<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

class FileSource extends AbstractSource
{
    /** @var SplFileInfo */
    private $dataFile;

    public function __construct(
        $sourcePathPrefix,
        SplFileInfo $dataFile,
        SplFileInfo $manifestFile = null,
        array $mapping = null
    ) {
        parent::__construct($sourcePathPrefix, $manifestFile, $mapping);

        $this->dataFile = $dataFile;
    }

    public function getSourceName()
    {
        return $this->dataFile->getBasename();
    }

    public function getSourceId()
    {
        return $this->dataFile->getPathname();
    }
}
