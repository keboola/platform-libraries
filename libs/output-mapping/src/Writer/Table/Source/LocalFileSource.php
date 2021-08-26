<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

class LocalFileSource implements SourceInterface
{
    /** @var SplFileInfo */
    private $file;

    public function __construct(SplFileInfo $file)
    {
        $this->file = $file;
    }

    public function getFile()
    {
        return $this->file;
    }

    public function getName()
    {
        return $this->file->getBasename();
    }

    public function isSliced()
    {
        return $this->file->isDir();
    }
}
