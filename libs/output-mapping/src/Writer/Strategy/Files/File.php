<?php

namespace Keboola\OutputMapping\Writer\Strategy\Files;

class File
{
    private $path;

    private $fileName;

    public function setFileName($filename)
    {
        $this->fileName = $filename;
        return $this;
    }

    public function getFileName()
    {
        return $this->fileName;
    }

    public function setPath($path)
    {
        $this->path = $path;
        return $this;
    }

    public function getPath()
    {
        return $this->path;
    }
}
