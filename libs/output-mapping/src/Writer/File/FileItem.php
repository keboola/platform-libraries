<?php

namespace Keboola\OutputMapping\Writer\File;

class FileItem
{
    /** @var string */
    private $name;

    /** @var string */
    private $path;

    /** @var string */
    private $pathName;

    /**
     * @param string $pathName
     * @param string $name Generalized name (for ABS this can include slashes)
     * @param string $path Generalized path (for ABS this can be an URL)
     */
    public function __construct($pathName, $path, $name)
    {
        $this->pathName = $pathName;
        $this->path = $path;
        $this->name = $name;
    }

    /**
     * @return string
     */
    public function getPathName()
    {
        return $this->pathName;
    }

    /**
     * @return string
     */
    public function getPath()
    {
        return $this->path;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }
}
